package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/weaviate/hdf5"
	"github.com/weaviate/weaviate/entities/models"
	weaviategrpc "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"google.golang.org/protobuf/proto"
)

// Filter catalog support for filtered-search hdf5 datasets that ship explicit
// allowlist bitmaps instead of one category per query (e.g.
// wiki-dpr-10m-e5b-filtered): filters/<group>/<name> is a np.packbits
// (big-endian) bitmap over the train rows and ground_truth/<group>/neighbors
// holds the filtered exact top-k, indexed by the position of <name> in
// ground_truth/<group>/names.
//
// A JSON manifest (see scripts/python/filter-catalog-manifest.py) lists the
// filters. On import every catalog filter becomes one boolean property:
//   - global filters:    f_<name> = true for members, queried with Equal true
//   - per_query filters: x_<name> = true for EXCLUDED rows, queried with IsNull
//     (these filters keep ~99% of the corpus, so storing the complement keeps
//     the import payload small; the class enables indexNullState for this).
// --filterSelect picks which filter(s) to query: "global:<name>" runs every
// test query against that filter, "per_query:<suffix>" runs each per-query
// filter whose name ends in <suffix> against its own query row.

type catalogManifestEntry struct {
	Name     string `json:"name"`
	Group    string `json:"group"` // "global" or "per_query"
	GTIndex  int    `json:"gt_index"`
	QueryRow int    `json:"query_row"` // per_query only, -1 otherwise
	Size     int    `json:"size"`
	Family   string `json:"family"`
	Tau      string `json:"tau"`
}

type catalogFilter struct {
	catalogManifestEntry
	Prop     string
	Inverted bool   // property marks excluded rows; query with IsNull
	bitmap   []byte // allowlist, only loaded for import
}

func (f *catalogFilter) allowed(row int) bool {
	return f.bitmap[row>>3]&(0x80>>uint(row&7)) != 0
}

type filterCatalog struct {
	Filters []*catalogFilter
	byName  map[string]*catalogFilter
}

func catalogPropName(e catalogManifestEntry) string {
	prefix := "f_"
	if e.Group == "per_query" {
		prefix = "x_"
	}
	return prefix + strings.NewReplacer(".", "_", "-", "_").Replace(e.Name)
}

func loadFilterCatalog(manifestPath string, ds *Hdf5Dataset, withBitmaps bool) *filterCatalog {
	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		log.Fatalf("Error reading filter catalog manifest: %v", err)
	}
	var entries []catalogManifestEntry
	if err := json.Unmarshal(raw, &entries); err != nil {
		log.Fatalf("Error parsing filter catalog manifest: %v", err)
	}
	cat := &filterCatalog{byName: map[string]*catalogFilter{}}
	for _, e := range entries {
		if e.Group != "global" && e.Group != "per_query" {
			log.Fatalf("filter %q: unknown group %q", e.Name, e.Group)
		}
		f := &catalogFilter{catalogManifestEntry: e, Prop: catalogPropName(e), Inverted: e.Group == "per_query"}
		if withBitmaps {
			f.bitmap = loadHdf5Uint8(ds.file, "filters/"+e.Group+"/"+e.Name)
			if len(f.bitmap)*8 < ds.trainRows {
				log.Fatalf("filter %q: bitmap covers %d rows, train has %d", e.Name, len(f.bitmap)*8, ds.trainRows)
			}
		}
		cat.Filters = append(cat.Filters, f)
		cat.byName[e.Name] = f
	}
	log.WithFields(log.Fields{"filters": len(cat.Filters), "bitmaps": withBitmaps}).Info("Loaded filter catalog")
	return cat
}

// schemaProperties returns one filterable boolean property per catalog filter.
func (c *filterCatalog) schemaProperties() []*models.Property {
	t, f := true, false
	props := make([]*models.Property, 0, len(c.Filters))
	for _, cf := range c.Filters {
		props = append(props, &models.Property{
			Name:              cf.Prop,
			DataType:          []string{"boolean"},
			IndexFilterable:   &t,
			IndexSearchable:   &f,
			IndexRangeFilters: &f,
		})
	}
	return props
}

// addProps sets the boolean membership properties of train row `row`.
func (c *filterCatalog) addProps(props map[string]interface{}, row int) {
	for _, f := range c.Filters {
		if f.allowed(row) != f.Inverted {
			props[f.Prop] = true
		}
	}
}

// selectQueries resolves a --filterSelect spec into parallel slices of query
// vectors, filtered ground truth and the filter to apply per query.
func (c *filterCatalog) selectQueries(spec string, ds *Hdf5Dataset) ([][]float32, [][]int, []*catalogFilter) {
	group, arg, ok := strings.Cut(spec, ":")
	if !ok {
		log.Fatalf("--filterSelect must be global:<name> or per_query:<name-suffix>, got %q", spec)
	}
	test := ds.TestVectors()
	gt, dims := loadHdf5Int64(ds.file, "ground_truth/"+group+"/neighbors")
	var queries [][]float32
	var neighbors [][]int
	var filters []*catalogFilter
	switch group {
	case "global":
		f := c.byName[arg]
		if f == nil || f.Group != "global" {
			log.Fatalf("unknown global filter %q", arg)
		}
		if len(dims) != 3 || int(dims[1]) != len(test) {
			log.Fatalf("unexpected ground_truth/global/neighbors dims %v for %d test vectors", dims, len(test))
		}
		q, k := int(dims[1]), int(dims[2])
		for i := 0; i < q; i++ {
			off := (f.GTIndex*q + i) * k
			queries = append(queries, test[i])
			neighbors = append(neighbors, gt[off:off+k])
			filters = append(filters, f)
		}
	case "per_query":
		if len(dims) != 2 {
			log.Fatalf("unexpected ground_truth/per_query/neighbors dims %v", dims)
		}
		k := int(dims[1])
		for _, f := range c.Filters {
			if f.Group != "per_query" || !(f.Name == arg || strings.HasSuffix(f.Name, arg)) {
				continue
			}
			if f.QueryRow < 0 || f.QueryRow >= len(test) {
				log.Fatalf("filter %q: query row %d out of range", f.Name, f.QueryRow)
			}
			queries = append(queries, test[f.QueryRow])
			neighbors = append(neighbors, gt[f.GTIndex*k:(f.GTIndex+1)*k])
			filters = append(filters, f)
		}
		if len(filters) == 0 {
			log.Fatalf("no per_query filter matches %q", arg)
		}
	default:
		log.Fatalf("unknown filter group %q", group)
	}
	log.WithFields(log.Fields{"spec": spec, "queries": len(queries), "filters": len(uniqueFilters(filters))}).Info("Selected catalog queries")
	return queries, neighbors, filters
}

func uniqueFilters(fs []*catalogFilter) map[string]bool {
	m := map[string]bool{}
	for _, f := range fs {
		m[f.Name] = true
	}
	return m
}

// nearVectorQueryGrpcCatalog is nearVectorQueryGrpc with a catalog filter.
func nearVectorQueryGrpcCatalog(cfg *Config, vec []float32, tenant string, f *catalogFilter) []byte {
	searchRequest := &weaviategrpc.SearchRequest{}
	if err := proto.Unmarshal(nearVectorQueryGrpc(cfg, vec, tenant, -1), searchRequest); err != nil {
		log.Fatalf("Failed to unmarshal grpc query: %v", err)
	}
	op := weaviategrpc.Filters_OPERATOR_EQUAL
	if f.Inverted {
		op = weaviategrpc.Filters_OPERATOR_IS_NULL
	}
	searchRequest.Filters = &weaviategrpc.Filters{
		TestValue: &weaviategrpc.Filters_ValueBoolean{ValueBoolean: true},
		On:        []string{f.Prop},
		Operator:  op,
	}
	data, err := proto.Marshal(searchRequest)
	if err != nil {
		log.Fatalf("grpc marshal err: %v", err)
	}
	return data
}

func loadHdf5Uint8(file *hdf5.File, name string) []byte {
	dataset, err := file.OpenDataset(name)
	if err != nil {
		log.Fatalf("Error opening dataset %s: %v", name, err)
	}
	defer dataset.Close()
	dims, _, _ := dataset.Space().SimpleExtentDims()
	datatype, err := dataset.Datatype()
	if err != nil {
		log.Fatalf("dataset %s: unable to read datatype: %v", name, err)
	}
	if len(dims) != 1 || datatype.Size() != 1 {
		log.Fatalf("dataset %s: expected 1-D uint8, got dims %v byte size %d", name, dims, datatype.Size())
	}
	data := make([]uint8, dims[0])
	if err := dataset.Read(&data); err != nil {
		log.Fatalf("Error reading dataset %s: %v", name, err)
	}
	return data
}

// loadHdf5Int64 reads an int64 dataset of any rank as a flat slice.
func loadHdf5Int64(file *hdf5.File, name string) ([]int, []uint) {
	dataset, err := file.OpenDataset(name)
	if err != nil {
		log.Fatalf("Error opening dataset %s: %v", name, err)
	}
	defer dataset.Close()
	dims, _, _ := dataset.Space().SimpleExtentDims()
	if getHDF5ByteSize(dataset) != 8 {
		log.Fatalf("dataset %s: expected int64", name)
	}
	n := uint(1)
	for _, d := range dims {
		n *= d
	}
	data := make([]int64, n)
	if err := dataset.Read(&data); err != nil {
		log.Fatalf("Error reading dataset %s: %v", name, err)
	}
	out := make([]int, n)
	for i, v := range data {
		out[i] = int(v)
	}
	return out, dims
}

func (c *Config) validateFilterCatalog() error {
	if c.FilterSelect != "" && c.FilterCatalog == "" {
		return fmt.Errorf("--filterSelect requires --filterCatalog")
	}
	if c.FilterCatalog != "" && c.Filter {
		return fmt.Errorf("--filterCatalog and --filter are mutually exclusive")
	}
	return nil
}
