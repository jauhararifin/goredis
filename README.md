
## Benchmark

redis-benchmark -h localhost -p 3100 -r 100000000000 -P 1000 -c 50 -t SET,GET
