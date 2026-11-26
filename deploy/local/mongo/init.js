const application = db.getSiblingDB('airtraffic');
application.createUser({user: 'publisher', pwd: process.env.MONGO_PUBLISHER_PASSWORD,
  roles: [{role:'readWrite',db:'airtraffic'}]});
application.createUser({user: 'reader', pwd: process.env.MONGO_READER_PASSWORD,
  roles: [{role:'read',db:'airtraffic'}]});
