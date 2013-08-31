#!/usr/bin/env node

var batcher = require('./auto-batch.js')(false, function(err, batchStream) {
  if (err) return console.error(err)
  process.stdin.pipe(batchStream)
})

