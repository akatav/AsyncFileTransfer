'use strict';

var cluster = require('cluster')
  , zmq = require('zmq');

var NBR_WORKERS = 3;

function randomBetween(min, max) {
  return Math.floor(Math.random() * (max - min) + min);
}

function randomString() {
  var source = 'abcdefghijklmnopqrstuvwxyz'
    , target = [];

  for (var i = 0; i < 20; i++) {
    target.push(source[randomBetween(0, source.length)]);
  }
  return target.join('');
}

function workerTask() {
  var dealer = zmq.socket('dealer');
  dealer.identity = randomString();
  console.log('I am a worker. My identity is: ' + dealer.identity);

  dealer.connect('tcp://localhost:5671');
  
  console.log(dealer.identity + ' connecting to master');

  var total = 0;

  var sendMessage = function () {
	//console.log(dealer.identity + ' sending a message to master');
    dealer.send(['', 'Hi Boss']);
  };
  
  var sendTaskFinishMessage = function(n) {
	//console.log(dealer.identity + ' sending a task finish message to master');
	dealer.send(['', 'Finished reading file ' + n]);
  };
  
  var askForChunk = function(offset, chunkSz, filePath) {
	dealer.send(['', offset, chunkSz, filePath]);
  };

  //  Get workload from broker, until finished
  dealer.on('message', function onMessage() {
    var args = Array.apply(null, arguments);
	var n = args.length;
	console.log('The master has sent me: ' + dealer.identity + ' the following message' );
	for(var i=0;i<n;i++) {
		console.log(i + "-->" + args[i].toString());
	}

    var workload = args[1].toString('utf8');
	var fileNum = args[2];
	
	if(fileNum != -1 && workload !== 'Complete assigned work!') {
		console.log('The master is asking me ' + dealer.identity  + ' to read the file: ' + fileNum);
		console.log('The file the master is asking me ' + dealer.identity + 'to read is at path: ' + workload);
		// send message to master asking for chunks
		askForChunk(offset, chunkSz, workload);
		setTimeout(function() { sendTaskFinishMessage(fileNum) }, 2000);
	}
	
	else if (workload === 'Complete assigned work!') {
		console.log('I ' + dealer.identity + ' already have work assigned. ' + args[2]);
	}

    else if (workload === 'EOF') { // or fileNum!=-1
      console.log('No more work for me! '+ dealer.identity);
      dealer.removeListener('message', onMessage);
      dealer.close();
      return;
    }
	
    total++;

    setTimeout(sendMessage, randomBetween(0, 500));
  });

  //  Tell the broker we're ready for work
  sendMessage();
}

function main() {
  var broker = zmq.socket('router');
  broker.bindSync('tcp://*:5671');

  var endTime = Date.now() + 5000
    , workersFired = 0
	, fileNum = 1;
	
  var workAssigned = new Array();
  var buf = new Array();

  broker.on('message', function () {
    var args = Array.apply(null, arguments)
      , identity = args[0]
      , now = Date.now();
	  
	console.log('A worker is sending me a message. It has the identity: ' + identity);
	console.log('The message had the following arguments');
	var n = args.length;
	for(var i=0;i<n;i++) {
		console.log(i + "-->" + args[i].toString());
	}
	
	var msg = args[2].toString('utf8');
	if(args.length==4) {
		// receiving a file chunk msg
		offset=args[1];
		chunkSz=args[2];
		filePath=args[3];
		fileNum = workAssigned[identity];
		if(offset < buf[fileNum].length && offset+chunkSz<buf[fileNum].length) {
			var oldoffset = offset;
			var newoffset = offset+chunkSz;
			var chunk=buf[fileNum].slice(oldoffset, newoffset).toString('binary');
			broker.send([identity, '', 'CHUNK', newoffset, chunk], zeromq.SND_MORE);
		}
		else {
			var chunk=buf.slice(offset, buf.length).toString('binary');
			broker.send([identity, '', 'CHUNK', -1, chunk]);
		}
	}
	else if(args.length==2) {
    if (fileNum <= 5) {
	  
	  if(msg.includes('Finished reading file')) {
		  // clear the workAssigned boolean for this dealer
		console.log(identity + ' has finished reading file');
		workAssigned[identity] = 0;
		
		// send more work
		console.log('Sending a file ' + fileNum  + ' to worker: ' + identity);
		workAssigned[identity] = fileNum;
		broker.send([identity, '', 'PATH', './files/'+ fileNum + '.zip', fileNum]);
		fileNum++;
	  }
	  else {
		
		  if(workAssigned[identity]>0) {
			  console.log('Worker ' + identity + ' already has file ' + workAssigned[identity] + ' to read');
			  console.log('Asking worker ' + identity + ' to complete assigned work with file ' + workAssigned[identity]);
			  broker.send([identity, '', 'TEXT', 'Complete assigned work!', workAssigned[identity]]);
		  }
		  else {
			  console.log('Sending a file ' + fileNum  + ' to worker: ' + identity);
			  workAssigned[identity] = fileNum;
		  	  buf[fileNum]=fs.readFileSync('./files/' + fileNum);

			  broker.send([identity, '', 'PATH', './files/'+ fileNum + '.zip', fileNum]);
			  fileNum++;
		  }
	  }
    } else {
	  console.log('No more files for: ' + identity);
      broker.send([identity, '', 'EOF', -1]);
	  workersFired++;
	  if(workersFired === NBR_WORKERS) {
		broker.close();
		cluster.disconnect();
	  }
    }
	}
  });

  for (var i=0;i<NBR_WORKERS;i++) {
	console.log('Creating worker ' + i);
    cluster.fork();
  }
}

if (cluster.isMaster) {
  console.log('Cluster in master mode');
  main();
} else  {
  console.log('Cluster in worker mode');
  workerTask();
}