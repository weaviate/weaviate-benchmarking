
These are alternate instructions for running the weaivate benchmarks

# Instructions for ann benchmarks

* clone this forked repository
* cd into the repo top-level directory
* checkout the appropriate branch if you are testing an unmerged branch
* mkdir benchmark-data (if needed)
* cd into "benchmark-data" and download the benchmark dataset for your task (if needed)
  * for example - wget http://ann-benchmarks.com/deep-image-96-angular.hdf5
* go back to top-level directory of repo
* edit docker-compose.yml with your weaviate port (if needed)
* edit benchmark-scripts/ann/benchmark.py with that Weaviate port (if needed)
* build/rebuid the benchmark docker container
  * build docker-compose build benchmark-ann-gsi
* in a separate terminal ( ideally behind 'screen' ), launch the weaviate container
  * docker-compose up weaviate
* in a separate terminal ( ideally behind 'screen' ), launch the benchmarrk container
  * docker-compose up benchmark-ann-gsi

# Troubleshooting

* if you ever need to start over, I found it was best to run these steps:
  * stop both containers if they aren't stopped
  * docker-compose down
  * restart the containers per the instructions above
* make sure the "results" and "weaviate-data" and "benchmark-data" are owned by you
  * sudo chown $USER.$USER -R results weaviate-data benchmark-data

