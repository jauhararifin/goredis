
## Golang Tests

```bash
# run the tests
go test . -v -coverprofile=cover.out

# show the coverage
go tool cover -html cover.out

# run the benchmark tests
go test . -v -bench=BenchmarkRedisSet -benchmem -benchtime=10s -memprofile=mem.out -cpuprofile=cpu.out -run="^$"

# using count instead of duration:
go test . -v -bench=BenchmarkRedisSet -benchmem -benchtime=20000000x -memprofile=mem.out -cpuprofile=cpu.out -run="^$"
```

## Benchmark

```bash
redis-benchmark -h localhost -p 3100 -r 100000000000 -P 1000 -c 50 -t SET,GET
```
