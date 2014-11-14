//---------------------------------------------------------------------------------------------------------------//
// Author - Ksenofontov Sergey
// Date of creation - 14.11.2014
//
// Main idea of the realization is to use additional service channel for speaker control.
//
// Minimum dependencies
// Used multi() for redis work optimization.
//---------------------------------------------------------------------------------------------------------------//

'use strict';

var redis = require('redis');

//connection settings
var ip = '10.0.0.1';
var port = 6379;
var errorBuffer = 'error-list';

// channels names
var serviceChannel = 'speakers';
var chatChannel = 'text-chat';

if (process.argv[2] === 'getErrors')
    getErrors();
else {
    new Checker(serviceChannel);
    new Listener(chatChannel);
}


function Checker(channel) {
    var connect = redis.createClient(port, ip);

    connect.on('ready' , function() {
        checkSpeakerChannel();
    });

    connect.on('subscribe', function(p, count) {

        // Do not check channel - Chacker works only with service channel
        onCheckerSubscribe(count);
    });

    function checkSpeakerChannel() {
        connect.pubsub(['NUMSUB', channel], function(err, res){
            if (res[1] === '0')
                setSpeaker(); // Subscribe to service channel
            else
                setTimeout(checkSpeakerChannel, 2000); // Check service channel later
        });
    }

    function setSpeaker() {
        connect.subscribe(channel);
    }

    function onCheckerSubscribe(count) {
        if (count === 1) {       // Again check count of subscribers
                                 // to catch simultaneous connect

            new Publisher(chatChannel); // publisher's call
        } else {
            connect.unsubscribe(channel);
            setTimeout(checkSpeakerChannel, 2000); // Start checking again
        }
    }
}

function Listener(channel) {
    var connect = redis.createClient(port, ip);
    var errorConnect = redis.createClient(port, ip); // Additional channel for errors

    connect.on('ready', function() {
        connect.subscribe(channel);
    });

    connect.on('message', function(msgChannel, message) {
        eventHandler(message, msgHandler);
    });

    function eventHandler(msg, callback){

        function onComplete(){
            var error = Math.random() > 0.85;

            callback(error, msg);
        }

        setTimeout(onComplete, Math.floor(Math.random()*1000));
    }

    function msgHandler(error, msg) {
        if (error) {
            console.log('Error! :', msg);
            errorConnect.rpush(errorBuffer, msg);
        } else
            console.log('Incoming message: ', msg);
    }

}

function Publisher(channel) {
    var connect = redis.createClient(port, ip);
    var self = this;

    connect.on('ready', setGenerator);

    function getMessage(){
        self.cnt = self.cnt || 0;
        return self.cnt++;
    }

    function setGenerator() {
        connect.publish(channel, getMessage());
        setTimeout(setGenerator, Math.floor(Math.random()*1000));
    }
}

function getErrors() {
    var connect = redis.createClient(port, ip);

    connect.llen(errorBuffer, function(error, count){
        connect
            .multi()
            .lrange([errorBuffer, 0, count], function(e, arr) {
                arr.forEach(function(v) {console.log(v)});
            })
            .ltrim([errorBuffer, count, -1], function(){})
            .exec(function(){
                connect.quit();
            });
    })
}
