# level-bulk-load

this is an attempt at maximizing write throughput with node + leveldb by implementing different approaches.

if you have optimizations, please pull request them in!

### approaches

the first one is `load-batches.js`

run it with:

```sh
time node load-batches.js -b 320000
```

this will create 320,000 10kb documents, which should be a leveldb of around 1.7 GB

see the `.log` files for the batch level results on my machine (Macbook Air 1.7gh Core i7, 4GB RAM)

there is also a `nobatch` result log that was just pure `.put` without any `.batch` for reference

on a usb 2.0 external HD it took ~15 minutes, on the internal SSD it took ~10 minutes

Options
---

There are a few options to let you tune your load test:

  * -b number of batches to insert (default: 25)
  * -l length of each record to insert (default: 10250)
  * -m writeBufferSize in MB (default: 16)
  * -n number of records per batch (default: `Math.floor(writeBufferSize * 1024 * 1024 / length)`)
