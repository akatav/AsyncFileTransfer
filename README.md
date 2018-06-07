# AsyncFileTransfer
Uses 0mq router-dealer pattern to transfer many files chunk by chunk across a network


Requires 0mq installation

Run RouterDealerExample-v2.js after installing the node modules. 

Utility: When large files are transferred over the network chunk by chunk, the router on one side needs to send the file chunks and maintain the right order and offsets. 
The client on the receiving end needs to recieve all the bits of data and convert them back to whichever file format it is and write to the local disk. 
