/*
 * Copyright 2019 Lightbend Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

const path = require("path");
const grpc = require("grpc");
const protoLoader = require("@grpc/proto-loader");
const fs = require("fs");

const debug = require("debug")("cloudstate");
// Bind to stdout
debug.log = console.log.bind(console);

module.exports = class CloudState {
  constructor() {
    try {
      this.proto = fs.readFileSync("user-function.desc");
    } catch (e) {
      console.error("Unable to load user-function.desc protobuf descriptor!");
      throw e;
    }

    this.entities = [];
  }

  addEntity(...entities) {
    this.entities = this.entities.concat(entities);
  }

  start(options) {
    const opts = {
      ...{
        bindAddress: "0.0.0.0",
        bindPort: 8080
      },
      ...options
    };

    const allEntitiesMap = {};
    this.entities.forEach(entity => {
      allEntitiesMap[entity.serviceName] = entity.service;
    });

    const entityTypes = {};
    this.entities.forEach(entity => {
      const entityServices = entity.register(allEntitiesMap);
      entityTypes[entityServices.entityType()] = entityServices;
    });

    const server = new grpc.Server();

    Object.values(entityTypes).forEach(services => {
      services.register(server);
    });

    const includeDirs = [
      path.join(__dirname, "..", "proto"),
      path.join(__dirname, "..", "protoc", "include")
    ];
    const packageDefinition = protoLoader.loadSync(path.join("cloudstate", "entity.proto"), {
      includeDirs: includeDirs
    });
    const grpcDescriptor = grpc.loadPackageDefinition(packageDefinition);

    const entityDiscovery = grpcDescriptor.cloudstate.EntityDiscovery.service;

    server.addService(entityDiscovery, {
      discover: this.discover.bind(this),
      reportError: this.reportError.bind(this)
    });

    const boundPort = server.bind(opts.bindAddress + ":" + opts.bindPort, grpc.ServerCredentials.createInsecure());
    server.start();
    console.log("gRPC server started on " + opts.bindAddress + ":" + boundPort);

    return boundPort;
  }

  discover(call, callback) {
    const protoInfo = call.request;
    debug("Discover call with info %o, sending %s entities", protoInfo, this.entities.length);
    const entities = this.entities.map(entity => {
      return {
        entityType: entity.entityType(),
        serviceName: entity.serviceName,
        persistenceId: entity.options.persistenceId
      };
    });
    callback(null, {
      proto: fs.readFileSync("user-function.desc"),
      entities: entities
    });
  }

  reportError(call, callback) {
    const msg = call.request.message;
    console.error("Error reported from sidecar: " + msg);
    callback(null, {});
  }
};