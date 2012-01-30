// The default port this process will listen to.  Must match the
// configuration of this node within Flume.  35853 is Flume's default
// port for an 'rpcSink'.
var DEFAULT_PORT = 35853;

var URL   = require('url');
var HTTP  = require('http');
var Flume = require('flume-rpc'); // https://github.com/recoset/node-flume-rpc
var fs    = require('fs');

// Handle the event as it comes off the line from the upstream Flume
// node. Events will look like this:
// 
//   { timestamp: 740123258,
//     priority: 3,
//     body: 'http://www.google.com',
//     nanos: 1278719169,
//     host: 'silicon',
//     fields: { tailSrcFile: 'file' } }
//   { timestamp: 740123258,
//     priority: 3,
//     body: 'http://blog.infochimps.com',
//     nanos: 1279034685,
//     host: 'silicon',
//     fields: { tailSrcFile: 'file' } }
function processEvent(event, output) {
  var url = event.body;
  if (url == undefined) {
    errorMessage("Missing URL");
    return;
  }
  scrapeURL(url, output);
}

// Scrape the given URL and complain if we don't get a 200.
function scrapeURL(url, output) {
  try {
    var urlObj = URL.parse(url);
  } catch (e) {
    errorMessage("Malformed\t" + url);
    return;
  }

  var req = HTTP.get(urlObj, function(response) {
    response.on('error', function(error) {
      errorMessage("Response\t" + url + "\t" + error.message);
    });
    if (response.statusCode == 200) {
      response.chunk_id = 1;
      response.on('data', function(chunk) {
	writeHTMLChunk(url, output, chunk, response);
      });
    } else {
      errorMessage(response.statusCode + "\t" + url);
    }
  });
  req.on('error', function(error) {
    errorMessage("Request\t" + url + "\t" + error.message);
  });
}

// Write a chunk of HTML decorated with the URL it came from to the
// output.
function writeHTMLChunk(url, output, chunk, response) {
  if (output == null) {
    console.log(url + "\t" + chunk.toString('utf8'));
  } else {
    var decoratedChunk = new Object();
    decoratedChunk.url       = url;
    decoratedChunk.chunk     = chunk.toString('utf8'); // FIXME is it safe to assume UTF8 encoding here always?
    decoratedChunk.chunk_id  = response.chunk_id;
    response.chunk_id       += 1;
    var decoratedBuffer = new Buffer(JSON.stringify(decoratedChunk) + "\n");
    fs.write(output, decoratedBuffer, 0, decoratedBuffer.length, null, function(err, written, buffer) {
      if (written != decoratedBuffer.length) {
	errorMessage("Bad write\t" + url);
      }
    });
  }
}

// Log an error message.
function errorMessage(message) {
  console.log("ERROR\t" + message);
}

// Open the output file.
function openOutput(path) {
  if (path == null) {
    console.log("Writing data to stdout");
    return;
  }
  try {
    console.log("Writing data to " + path);
    return fs.openSync(path, 'a');
  } catch (e) {
    console.log("Could not open " + path + " for writing.");
    process.exit();
  }
}

// Start this thing up!
function main() {
  var port   = (process.argv.length > 2 ? parseInt(process.argv[2])   : DEFAULT_PORT);
  console.log("Flume sink at port " + port);
  var output = (process.argv.length > 3 ? openOutput(process.argv[3]) : null)
  var sink   = new Flume.Sink;
  sink.on('message', function(event) { processEvent(event, output) });
  sink.on('close', function(success) { this.close();  success();   });
  sink.listen(port);
}

main();
