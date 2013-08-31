var level = require('level')
var fs = require('fs')
var path = require('path')

var data = fs.readFileSync(path.join(__dirname, '10kilobytesof.json')).toString()

var bufferSize = 16 // megabytes
var size = 100 * bufferSize
var batches = process.argv[2] || 25

console.log('loading', batches, 'batches of', size)

var db = level(path.join(process.cwd(), 'test.db'), {
  writeBufferSize: 1024 * 1024 * bufferSize
})

load()

function load() {
  batches--
  if (batches <= 0) return
  putBatch(load)
}

function putBatch(cb) {
  console.time('batch of ' + size)
  var pending = 0
  for (var i = 0; i < size; i++) {
    pending++
    db.put(i + '-' + +new Date(), data, function(err) {
      if (err) console.error(err)
      pending--
      if (pending === 0) {
        console.timeEnd('batch of ' + size)
        cb()
      }
    })
  }
}


