var level = require('level')
var fs = require('fs')
var path = require('path')
var stats = require('stats-lite')

var data = fs.readFileSync(path.join(__dirname, '10kilobytesof.json')).toString()

var argv = require('optimist')
  .default('b', 25) // batches
  .default('m', 16) // buffer size in mb
  .default('l', data.length) // record length (btyes)
  .argv

var writeBufferSize = argv.m * 1024 * 1024
var batches = argv.b
var recordLength = +argv.l
if (recordLength > writeBufferSize)
  throw new Error("records can't be larger than the writeBufferSize of %s", writeBufferSize)

if (recordLength > data.length) {
  var appends = recordLength / data.length
  var copy = data
  for (var i = 0; i < appends; i++) {
    copy += data
  }
  data = copy
}

var size = +argv.n || Math.floor(writeBufferSize / recordLength)

var dataBuffer = new Buffer(data)
function next() {
  if (recordLength == data.length) return data
  var slice = dataBuffer.slice(0, recordLength)
  dataBuffer = Buffer.concat([dataBuffer.slice(recordLength), slice])
  return slice.toString()
}

console.log('loading %s batches of %s records', batches, size)
console.log('%s bytes per batch', size * recordLength)

var db = level(path.join(process.cwd(), 'test.db'), {
  writeBufferSize: writeBufferSize
}, load)

function load() {
  batches--
  if (batches <= 0) {
    console.log("max: %s", Math.max.apply(null, runs))
    console.log("avg: %s", stats.mean(runs))
    console.log("std dev: %s", stats.stdev(runs))
    console.log("25th percentile: %s", stats.percentile(runs, 0.25))
    console.log("median: %s", stats.median(runs))
    console.log("75th percentile: %s", stats.percentile(runs, 0.75))
    return
  }
  putBatch(load)
}

var runs = []
function putBatch(cb) {
  var start = Date.now()
  console.time('batch of ' + size)
  var batch = db.batch()
  for (var i = 0; i < size; i++) batch.put(i + '-' + Date.now(), next())
  batch.write(function() {
    console.timeEnd('batch of ' + size)
    var elapsed = Date.now() - start
    runs.push(elapsed)
    cb()
  })
}
