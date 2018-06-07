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
	console.log(dealer.identity + ' sending a message to master');
    dealer.send(['', 'Hi Boss']);
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

    if (workload === 'Fired!') {
      console.log('Completed: '+total+' tasks ('+dealer.identity+')');
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

  var endTime = Date.now() + 30000
    , workersFired = 0
	, fileNum = 1;

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
	
    if (now < endTime && fileNum <= 5) {
	  console.log('Sending a message to worker: ' + identity);
      broker.send([identity, '', './files/'+ fileNum + '.zip']);
	  fileNum++;
    } else {
	  console.log('Firing worker with identity: ' + identity);
      broker.send([identity, '', 'Fired!']);
      workersFired++;
      if (workersFired === NBR_WORKERS) {
        setImmediate(function () {
          broker.close();
          cluster.disconnect();
        });
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