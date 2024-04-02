package cmd

import (
	"bytes"
	"testing"
)

// func TestWithWeaviate(t *testing.T) {
// 	ctx := context.Background()
// 	req := testcontainers.ContainerRequest{
// 		Image:        "semitechnologies/weaviate:1.23.7",
// 		Env:          map[string]string{"AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED": "true", "PERSISTENCE_DATA_PATH": "/var/lib/weaviate"},
// 		ExposedPorts: []string{"50051/tcp", "8080/tcp"},
// 		WaitingFor:   wait.ForLog("Serving weaviate"),
// 	}
// 	weaviateContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
// 		ContainerRequest: req,
// 		Started:          true,
// 	})
// 	if err != nil {
// 		log.Fatalf("Could not start redis: %s", err)
// 	}

// 	ports, err := weaviateContainer.Ports(ctx)
// 	if err != nil {
// 		log.Fatalf("Could not get ports: %s", err)
// 	}
// 	fmt.Printf("Port: %v\n", ports)

// 	defer func() {
// 		if err := weaviateContainer.Terminate(ctx); err != nil {
// 			log.Fatalf("Could not stop redis: %s", err)
// 		}
// 	}()
// }

func TestAnnBenchmarkCommand(t *testing.T) {

	rootCmd.SetArgs([]string{
		"ann-benchmark",
		"--vectors=/Users/trengrj/datasets/fiqa-12k-384-dot.hdf5",
		"--distance=dot",
		"--indexType=hnsw"})

	// initAnnBenchmark()

	var out bytes.Buffer

	annBenchmarkCommand.SetOut(&out)

	// Execute the command
	if err := annBenchmarkCommand.Execute(); err != nil {
		t.Errorf("Failed to execute annBenchmarkCommand: %v", err)
	}

	expectedOutput := "some expected output" // Define your expected output
	if out.String() != expectedOutput {
		t.Errorf("Unexpected command output:\nExpected: %s\nGot: %s", expectedOutput, out.String())
	}

}
