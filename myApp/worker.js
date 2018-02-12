var SCWorker = require('socketcluster/scworker');
var fs = require('fs');
var express = require('express');
var serveStatic = require('serve-static');
var path = require('path');
var morgan = require('morgan');
var healthChecker = require('sc-framework-health-check');

class Worker extends SCWorker {
    run() {
        console.log('   >> Worker PID:', process.pid);
        var environment = this.options.environment;

        var app = express();

        var httpServer = this.httpServer;
        var scServer = this.scServer;

        if (environment == 'dev') {
            // Log every HTTP request. See https://github.com/expressjs/morgan for other
            // available formats.
            app.use(morgan('dev'));
        }
        app.use(serveStatic(path.resolve(__dirname, 'public')));

        // Add GET /health-check express route
        healthChecker.attach(this, app);

        httpServer.on('request', app);

        var count = 0;

        /*
          In here we handle our incoming realtime connections and listen for events.
        */
        scServer.on('connection', function (socket) {
            socket.on('connect' , () => {
                console.log('user connect');
            });
            socket.on('authentication', function (data,respond) {
                if (data = 'sa') {
                    socket.setAuthToken({username: data});
                    respond();
                } else {
                    console.log('fail login')
                }
            });
            socket.on('chat', function (data) {
                scServer.exchange.publish('startchat', data)
            });
            scServer.addMiddleware(scServer.MIDDLEWARE_PUBLISH_IN, function (req, next) {
                var authToken = req.socket.authToken;
                if (authToken) {
                    console.log(authToken)
                    next();
                } else {
                    next('You are not authorized to publish to ' + req.channel);
                }
            });

        });

    }
}

new Worker();
