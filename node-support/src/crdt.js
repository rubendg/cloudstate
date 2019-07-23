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
const fs = require("fs");
const util = require("util");
const loadProtobuf = require("protobuf-helper");
const grpc = require("grpc");
const protoLoader = require("@grpc/proto-loader");
const CloudState = require("./cloudstate");
const AnySupport = require("./protobuf-any");

// All of these classes are implemented the old fashioned way, using functions, and without prototype methods.
// This is to allow us to have variables (held in the context of the new function evaluation), the reason for
// doing that is to ensure people with debuggers etc don't find and try to edit these directly, if they did,
// it was seriously mess up the logic.

function GCounter() {
  let currentValue = 0;
  let delta = 0;

  Object.defineProperty(this, "value", {
    get: function () {
      return currentValue;
    }
  });

  this.increment = function (increment) {
    if (increment < 0) {
      throw new Error("Cannot decrement a GCounter");
    }
    currentValue += increment;
    delta += increment;
    return this;
  };

  this.getAndResetDelta = function () {
    if (delta > 0) {
      const crdtDelta = {
        gcounter: {
          increment: delta
        }
      };
      delta = 0;
      return crdtDelta;
    } else {
      return null;
    }
  };

  this.applyDelta = function (delta) {
    if (delta.gcounter === undefined) {
      throw new Error(util.format("Cannot apply delta %o to GCounter", delta));
    }
    currentValue += delta.gcounter.increment;
  };

  this.getStateAndResetDelta = function () {
    delta = 0;
    return {
      gcounter: {
        value: currentValue
      }
    };
  };

  this.applyState = function (state) {
    if (state.gcounter === undefined) {
      throw new Error(util.format("Cannot apply state %o to GCounter", state));
    }
    currentValue = state.gcounter.value;
  };

  this.toString = function () {
    return "GCounter(" + currentValue + ")";
  };
}

function PNCounter() {
  let currentValue = 0;
  let delta = 0;

  Object.defineProperty(this, "value", {
    get: function () {
      return currentValue;
    }
  });

  this.increment = function (increment) {
    currentValue += increment;
    delta += increment;
    return this;
  };

  this.decrement = function (increment) {
    currentValue -= increment;
    delta -= increment;
    return this;
  };

  this.getAndResetDelta = function () {
    if (delta !== 0) {
      const crdtDelta = {
        pncounter: {
          change: delta
        }
      };
      delta = 0;
      return crdtDelta;
    } else {
      return null;
    }
  };

  this.applyDelta = function (delta) {
    if (delta.pncounter === undefined) {
      throw new Error(util.format("Cannot apply delta %o to PNCounter", delta));
    }
    currentValue += delta.pncounter.change;
  };


  this.getStateAndResetDelta = function () {
    delta = 0;
    return {
      pncounter: {
        value: currentValue
      }
    };
  };

  this.applyState = function (state) {
    if (state.pncounter === undefined) {
      throw new Error(util.format("Cannot apply state %o to PNCounter", state));
    }
    currentValue = state.pncounter.value;
  };

  this.toString = function () {
    return "PNCounter(" + currentValue + ")";
  };
}

function GSet() {
  // Map of elements to keys
  let currentValue = new Map();
  let keyedValues = {};
  let delta = {};

  this.has = function (element) {
    return currentValue.has(element);
  };

  Object.defineProperty(this, "size", {
    get: function () {
      return currentValue.size;
    }
  });

  this.forEach = function (callback, thisArg) {
    return currentValue.keys().forEach(callback, thisArg);
  };

  this[Symbol.iterator] = function () {
    return currentValue.keys()[Symbol.iterator]();
  };

  /**
   * Add an element to this set.
   *
   * @param key A unique, stable key for the element, or, the element itself if it's a String.
   * @param element The element.
   */
  this.add = function (key, element) {
    // todo validate
    if (key === null || key === undefined) {
      throw new Error("Cannot add null to GSet")
    } else if (!(typeof key === "string")) {
      throw new Error("Key must be a string! To insert non string elements into the set, select a unique, stable " +
        "string key for the element value, which can be used by CloudState to determine equality of the serialized " +
        "value")
    }

    let serializedElement;
    if (element === undefined || element === null) {
      element = key;
      serializedElement = null;
    } else {
      serializedElement = AnySupport.serialize(element);
    }
    if (!currentValue.has(element)) {
      if (keyedValues[key] !== undefined) {
        throw new Error(util.format("Element %o is not in the set, however, its key, %s, is in the set with a value of %o", element, key, keyedValues[key]));
      }
      currentValue.set(element, key);
      keyedValues[key] = element;
      delta[key] = serializedElement;
    } else if (currentValue.get(element) !== key) {
      throw new Error(util.format("Element %o is in the set, however, it is in there with a key of [%s], instead of [%s]. This usually means the key function is unstable.", element, currentValue.get(element), key));
    }
    return this;
  };

  this.getAndResetDelta = function () {
    const deltaKeys = Object.keys(delta);
    if (deltaKeys.size > 0) {
      const added = deltaKeys.map(key => {
        const value = delta[key];
        if (value === null) {
          return {
            key: key
          };
        } else {
          return {
            key: key,
            value: value
          };
        }
      });
      const crdtDelta = {
        gset: {
          added: added
        }
      };
      delta = {};
      return crdtDelta;
    } else {
      return null;
    }
  };

  this.applyDelta = function (delta, anySupport) {
    if (delta.gset === undefined) {
      throw new Error(util.format("Cannot apply delta %o to GSet", delta));
    }
    delta.gset.added.forEach(element => {
      let value;
      if (element.value === undefined || element.value === null) {
        value = element.key;
      } else {
        value = anySupport.deserialize(element.value);
      }
      currentValue.set(value, key);
      keyedValues[key] = value;
    });
  };

  this.getStateAndResetDelta = function () {
    delta = {};
    const items = currentValue.keys().map(value => {
      const key = items.get(value);
      if (value === key) {
        return {
          key: key
        };
      } else {
        return {
          key: key,
          value: AnySupport.serialize(value)
        };
      }
    });
    return {
      gset: {
        items: items
      }
    };
  };

  this.applyState = function (state, anySupport) {
    if (state.gset === undefined) {
      throw new Error(util.format("Cannot apply state %o to GSet", state));
    }
    currentValue.clear();
    keyedValues = {};
    state.gset.items.forEach(element => {
      let value;
      if (element.value === undefined || element.value === null) {
        value = element.key;
      } else {
        value = anySupport.deserialize(element.value);
      }
      currentValue.set(value, key);
      keyedValues[key] = value;
    });
  };

  this.toString = function () {
    return "GSet(" + Array.from(currentValue.keys()).join(",") + ")";
  };
}

function ORSet() {
  let currentValue = new Map();
  let keyedValues = {};
  let delta = {
    added: {},
    removed: new Set(),
    cleared: false
  };

  this.has = function (element) {
    return currentValue.has(element);
  };

  Object.defineProperty(this, "size", {
    get: function () {
      return currentValue.size;
    }
  });

  this.forEach = function (callback, thisArg) {
    return currentValue.keys().forEach(callback, thisArg);
  };

  this[Symbol.iterator] = function () {
    return currentValue.keys()[Symbol.iterator]();
  };

  /**
   * Add an element to this set.
   *
   * @param key A unique, stable key for the element, or, the element itself if it's a String.
   * @param element The element.
   */
  this.add = function (key, element) {
    // todo validate
    if (key === null || key === undefined) {
      throw new Error("Cannot add null to GSet")
    } else if (!(typeof key === "string")) {
      throw new Error("Key must be a string! To insert non string elements into the set, select a unique, stable " +
        "string key for the element value, which can be used by CloudState to determine equality of the serialized " +
        "value")
    }

    let serializedElement;
    if (element === undefined || element === null) {
      element = key;
      serializedElement = null;
    } else {
      serializedElement = AnySupport.serialize(element);
    }
    if (!currentValue.has(element)) {
      if (keyedValues[key] !== undefined) {
        throw new Error(util.format("Element %o is not in the set, however, its key, %s, is in the set with a value of %o", element, key, keyedValues[key]));
      }
      if (delta.removed.has(key)) {
        delta.removed.delete(key)
      } else {
        delta.added[key] = serializedElement;
      }
      currentValue.set(element, key);
      keyedValues[key] = element;
    } else if (currentValue.get(element) !== key) {
      throw new Error(util.format("Element %o is in the set, however, it is in there with a key of [%s], instead of [%s]. This usually means the key function is unstable.", element, currentValue.get(element), key));
    }
    return this;
  };

  this.delete = function (element) {
    const key = currentValue.get(element);
    if (key !== null) {
      if (currentValue.size === 1) {
        this.clear();
      } else {
        currentValue.delete(element);
        delete keyedValues[key];
        if (delta.added[key] !== undefined) {
          delete delta.added[key];
        } else {
          delta.removed.add(key);
        }
      }
    }
    return this;
  };

  this.clear = function () {
    if (currentValue.size > 0) {
      delta.cleared = true;
      delta.added = {};
      delta.removed.clear();
      keyedValues = {};
      currentValue.clear();
    }
    return this;
  };

  this.getAndResetDelta = function () {
    const addedKeys = Object.keys(delta.added);
    if (delta.cleared || addedKeys.size > 0 || delta.removed.size > 0) {
      const added = addedKeys.map(key => {
        const value = delta[key];
        if (value === null) {
          return {
            key: key
          };
        } else {
          return {
            key: key,
            value: value
          };
        }
      });
      const removed = Array.from(delta.removed);
      const crdtDelta = {
        orset: {
          cleared: delta.cleared,
          removed: removed,
          added: added
        }
      };
      delta.cleared = false;
      delta.added = {};
      delta.removed.clear();
      return crdtDelta;
    } else {
      return null;
    }
  };

  this.applyDelta = function (delta, anySupport) {
    if (delta.orset === undefined) {
      throw new Error(util.format("Cannot apply delta %o to ORSet", delta));
    }
    if (delta.orset.cleared) {
      currentValue.clear();
      keyedValues = {};
    }
    delta.orset.removed.forEach(key => {
      const element = keyedValues[key];
      if (element !== null) {
        delete keyedValues[key];
        currentValue.delete(element);
      } else {
        debug("Delta instructed to delete key [%s], but it wasn't in the ORSet.", key)
      }
    });
    delta.orset.added.forEach(element => {
      let value;
      if (element.value === undefined || element.value === null) {
        value = element.key;
      } else {
        value = anySupport.deserialize(element.value);
      }
      if (currentValue.has(value)) {
        debug("Delta instructed to add value [%s, %o], but it's already present in the ORSet with key [%s].", element.key, value, currentValue.get(value));
      } else if (keyedValues[element.key] !== undefined) {
        debug("Delta instructed to add value [%s, %o], but that key is already present in the ORSet with value [%o].", element.key, value, keyedValues[element.key]);
      }
      currentValue.set(value, element.key);
      keyedValues[element.key] = value;
    });
  };

  this.getStateAndResetDelta = function () {
    delta = {};
    const items = currentValue.keys().map(value => {
      const key = items.get(value);
      if (value === key) {
        return {
          key: key
        };
      } else {
        return {
          key: key,
          value: AnySupport.serialize(value)
        };
      }
    });
    return {
      orset: {
        items: items
      }
    };
  };

  this.applyState = function (state, anySupport) {
    if (state.orset === undefined) {
      throw new Error(util.format("Cannot apply state %o to ORSet", state));
    }
    currentValue.clear();
    keyedValues = {};
    state.orset.items.forEach(element => {
      let value;
      if (element.value === undefined || element.value === null) {
        value = element.key;
      } else {
        value = anySupport.deserialize(element.value);
      }
      currentValue.set(value, key);
      keyedValues[key] = value;
    });
  };

  this.toString = function () {
    return "ORSet(" + Array.from(currentValue.keys()).join(",") + ")";
  };
}

function LWWRegister(value, clock = crdtServices.clocks.DEFAULT, customClockValue = 0) {
  // Make sure the value can be serialized.
  AnySupport.serialize(value);
  let currentValue = value;
  let currentClock = clock;
  let currentCustomClockValue = customClockValue;
  let delta = {
    value: null,
    clock: null,
    customClockValue: 0
  };

  Object.defineProperty(this, "value", {
    get: function () {
      return currentValue;
    },
    set: function (value) {
      this.setWithClock(value)
    }.bind(this)
  });

  /**
   * Set the the value.
   *
   * @param value The value to set.
   * @param clock The clock.
   * @param customClockValue Ignored if a custom clock isn't specified.
   */
  this.setWithClock = function (value, clock = crdtServices.clocks.DEFAULT, customClockValue = 0) {
    delta.value = AnySupport.serialize(value);
    if (clock !== undefined) {
      delta.clock = clock;
      delta.customClockValue = customClockValue;
    }
    currentValue = value;
    currentClock = clock;
    currentCustomClockValue = customClockValue;
    return this;
  };

  this.getAndResetDelta = function () {
    if (delta.value !== null) {
      const toReturn = delta;
      delta = {
        value: null,
        clock: null,
        customClockValue: 0
      };
      return {
        lwwregister: toReturn
      };
    } else {
      return null;
    }
  };

  this.applyDelta = function (delta, anySupport) {
    if (delta.lwwregister === undefined) {
      throw new Error(util.format("Cannot apply delta %o to LWWRegister", delta));
    }
    currentValue = anySupport.deserialize(delta.lwwregister.value);
  };

  this.getStateAndResetDelta = function () {
    delta = {
      value: null,
      clock: null,
      customClockValue: 0
    };
    return {
      lwwregister: {
        value: AnySupport.serialize(currentValue),
        clock: currentClock,
        customClockValue: currentCustomClockValue
      }
    };
  };

  this.applyState = function (state, anySupport) {
    if (state.lwwregister === undefined) {
      throw new Error(util.format("Cannot apply state %o to ORSet", state));
    }
    currentValue = anySupport.deserialize(state.lwwregister.value);
    currentClock = state.lwwregister.clock;
    currentCustomClockValue = state.lwwregister.customClockValue;
  };

  this.toString = function () {
    return "LWWRegister(" + currentValue + ")";
  };
}

function Flag() {
  let currentValue = false;
  let delta = false;

  Object.defineProperty(this, "value", {
    get: function () {
      return currentValue;
    }
  });

  this.enable = function () {
    if (!currentValue) {
      currentValue = true;
      delta = true;
    }
    return this;
  };

  this.getAndResetDelta = function () {
    if (delta) {
      delta = false;
      return {
        flag: {
          value: true
        }
      };
    } else {
      return null;
    }
  };

  this.applyDelta = function (delta) {
    if (delta.flag === undefined) {
      throw new Error(util.format("Cannot apply delta %o to Flag", delta));
    }
    currentValue = currentValue || delta.flag.value;
  };

  this.getStateAndResetDelta = function () {
    delta = false;
    return {
      flag: {
        value: currentValue
      }
    };
  };

  this.applyState = function (state) {
    if (state.flag === undefined) {
      throw new Error(util.format("Cannot apply state %o to Flag", state));
    }
    currentValue = state.flag.value;
  };

  this.toString = function () {
    return "Flag(" + currentValue + ")";
  };
}

class CrdtServices {
  constructor() {
    this.services = {};
    this.includeDirs = [
      path.join(__dirname, "..", "proto"),
      path.join(__dirname, "..", "protoc", "include")
    ];

    const root = loadProtobuf(path.join("cloudstate", "crdt.proto"), this.includeDirs);
    this.clocks = root.lookupEnum("cloudstate.crdt.CrdtClock").values;
    this.Empty = root.lookupType("google.protobuf.Empty");
  }

  addService(entity, allEntities) {
    this.services[entity.serviceName] = new CrdtSupport(entity.root, entity.service, entity.commandHandlers, allEntities);
  }

  entityType() {
    return "cloudstate.crdt.Crdt";
  }

  register(server) {
    const packageDefinition = protoLoader.loadSync(path.join("cloudstate", "crdt.proto"), {
      includeDirs: this.includeDirs
    });
    const grpcDescriptor = grpc.loadPackageDefinition(packageDefinition);

    const entityService = grpcDescriptor.cloudstate.crdt.Crdt.service;

    server.addService(entityService, {
      handle: this.handle.bind(this)
    });
  }

  handle(call) {
    let service;

    call.on("data", crdtStreamIn => {
      if (crdtStreamIn.init) {
        if (service != null) {
          service.streamDebug("Terminating entity due to duplicate init message.");
          console.error("Terminating entity due to duplicate init message.");
          call.write({
            failure: {
              description: "Init message received twice."
            }
          });
          call.end();
        } else if (crdtStreamIn.init.serviceName in this.services) {
          service = this.services[crdtStreamIn.init.serviceName].create(call, crdtStreamIn.init);
        } else {
          console.error("Received command for unknown CRDT service: '%s'", crdtStreamIn.init.serviceName);
          call.write({
            failure: {
              description: "CRDT service '" + crdtStreamIn.init.serviceName + "' unknown."
            }
          });
          call.end();
        }
      } else if (service != null) {
        service.onData(crdtStreamIn);
      } else {
        console.error("Unknown message received before init %o", crdtStreamIn);
        call.write({
          failure: {
            description: "Unknown message received before init"
          }
        });
        call.end();
      }
    });

    call.on("end", () => {
      if (service != null) {
        service.onEnd();
      } else {
        call.end();
      }
    });
  }
}

const crdtServices = new CrdtServices();

class ContextFailure extends Error {
  constructor(msg) {
    super(msg);
    if (Error.captureStackTrace) {
      Error.captureStackTrace(this, ContextFailure);
    }
    this.name = "ContextFailure";
  }
}

class CrdtSupport {

  constructor(root, service, commandHandlers, allEntities) {
    this.root = root;
    this.service = service;
    this.anySupport = new AnySupport(this.root);
    this.commandHandlers = commandHandlers;
    this.allEntities = allEntities;
  }

  create(call, init) {
    const handler = new CrdtHandler(this, call, init.entityId);
    if (init.state) {
      handler.handleState(init.state)
    }
    return handler;
  }
}

/**
 * Handler for a single CRDT entity.
 */
class CrdtHandler {

  /**
   * @param {CrdtSupport} support
   * @param call
   * @param entityId
   */
  constructor(support, call, entityId) {
    this.entity = support;
    this.call = call;
    this.entityId = entityId;

    this.currentState = null;

    this.streamId = Math.random().toString(16).substr(2, 7);

    this.streamDebug("Started new stream")
  }

  streamDebug(msg, ...args) {
    debug("%s [%s] - " + msg, ...[this.streamId, this.entityId].concat(args));
  }

  instantiateCrdtForState(state) {
    switch (state.state) {
      case "gcounter":
        return new GCounter();
      case "pncounter":
        return new PNCounter();
      case "gset":
        return new GSet();
      case "orset":
        return new ORSet();
      case "lwwregister":
        // It needs to be initialised with a value
        return new LWWRegister(crdtServices.Empty.create({}));
      case "flag":
        return new Flag();
      default:
        throw new Error(util.format("Unknown CRDT: %o", state))
    }
  }

  handleState(state) {
    this.streamDebug("Handling state %s", state.state);
    if (this.currentState === null) {
      this.currentState = this.instantiateCrdtForState(state);
    }
    this.currentState.applyState(state, this.support.anySupport);
  }

  onData(crdtStreamIn) {
    try {
      this.handleCrdtStreamIn(crdtStreamIn);
    } catch (err) {
      this.streamDebug("Error handling message, terminating stream: %o", crdtStreamIn);
      console.error(err);
      this.call.write({
        failure: {
          commandId: 0,
          description: "Fatal error handling message, check user container logs."
        }
      });
      this.call.end();
    }
  }

  handleCrdtStreamIn(crdtStreamIn) {
    switch (crdtStreamIn.message) {
      case "state":
        this.handleState(crdtStreamIn.state);
        break;
      case "changed":
        this.streamDebug("Received delta for CRDT type %s", crdtStreamIn.changed.delta);
        this.currentState.applyDelta(crdtStreamIn.changed, this.support.anySupport);
        break;
      case "deleted":
        this.streamDebug("CRDT deleted");
        break;
      case "command":
        this.handleCommand(crdtStreamIn.command);
        break;
      default:
        this.call.write({
          failure: {
            commandId: 0,
            description: util.format("Unknown message: %o", crdtStreamIn)
          }
        });
        this.call.end();
    }
  }

  serializeEffect(method, message) {
    let serviceName, commandName;
    // We support either the grpc method, or a protobufjs method being passed
    if (typeof method.path === "string") {
      const r = new RegExp("^/([^/]+)/([^/]+)$").exec(method.path);
      if (r == null) {
        throw new Error(util.format("Not a valid gRPC method path '%s' on object '%o'", method.path, method));
      }
      serviceName = r[1];
      commandName = r[2];
    } else if (method.type === "rpc") {
      serviceName = method.parent.name;
      commandName = method.name;
    }

    const service = this.entity.allEntities[serviceName];

    if (service !== undefined) {
      const command = service.methods[commandName];
      if (command !== undefined) {
        const payload = this.entity.serialize(command.resolvedRequestType.create(message));
        return {
          serviceName: serviceName,
          commandName: commandName,
          payload: payload
        };
      } else {
        throw new Error(util.format("Command [%s] unknown on service [%s].", commandName, serviceName))
      }
    } else {
      throw new Error(util.format("Service [%s] has not been registered as an entity in this user function, and so can't be used as a side effect or forward.", service))
    }
  }

  serializeSideEffect(method, message, synchronous) {
    const msg = this.serializeEffect(method, message);
    msg.synchronous = synchronous;
    return msg;
  }

  handleCommand(command) {
    const commandDebug = (msg, ...args) => {
      debug("%s [%s] (%s) - " + msg, ...[this.streamId, this.entityId, command.id].concat(args));
    };

    commandDebug("Received command '%s' with type '%s'", command.name, command.payload.type_url);

    if (!this.entity.service.methods.hasOwnProperty(command.name)) {
      commandDebug("Command '%s' unknown", command.name);
      this.call.write({
        failure: {
          commandId: command.id,
          description: "Unknown command named " + command.name
        }
      })
    } else {

      try {
        const grpcMethod = this.entity.service.methods[command.name];

        // todo maybe reconcile whether the command URL of the Any type matches the gRPC response type
        let commandBuffer = command.payload.value;
        if (typeof commandBuffer === "undefined") {
          commandBuffer = new Buffer(0)
        }
        const deserCommand = grpcMethod.resolvedRequestType.decode(commandBuffer);

        if (this.support.commandHandlers.hasOwnProperty(command.name)) {

          const effects = [];
          let active = true;
          const ensureActive = () => {
            if (!active) {
              throw new Error("Command context no longer active!");
            }
          };
          let deleted = false;
          let error = null;
          let reply;
          let forward = null;
          const noState = this.currentState === null;

          try {
            const ctx = {
              entityId: this.entityId,
              delete: () => {
                ensureActive();
                if (this.currentState === null) {
                  throw new Error("Can't delete entity that hasn't been created.");
                } else if (noState) {
                  this.currentState = null;
                } else {
                  deleted = true;
                }
              },
              fail: (msg) => {
                ensureActive();
                // We set it here to ensure that even if the user catches the error, for
                // whatever reason, we will still fail as instructed.
                error = new ContextFailure(msg);
                // Then we throw, to end processing of the command.
                throw error;
              },
              effect: (method, message, synchronous = false) => {
                ensureActive();
                effects.push(this.serializeSideEffect(method, message, synchronous))
              },
              thenForward: (method, message) => {
                forward = this.serializeEffect(method, message);
              }
            };
            Object.defineProperty(ctx, "state", {
              get: () => {
                ensureActive();
                return this.currentState;
              },
              set: (state) => {
                ensureActive();
                if (this.currentState !== null) {
                  throw new Error("Cannot create a new CRDT after it's been created.")
                } else if (typeof state.getAndResetDelta !== "function") {
                  throw new Error(util.format("%o is not a CRDT", state));
                } else {
                  this.currentState = state;
                }
              }
            });

            reply = this.support.commandHandlers[command.name](deserCommand, ctx);
          } catch (err) {
            if (error == null) {
              // If the error field isn't null, then that means we were explicitly told
              // to fail, so we can ignore this thrown error and fail gracefully with a
              // failure message. Otherwise, we rethrow, and handle by closing the connection
              // higher up.
              throw err;
            }
          } finally {
            active = false;
          }

          if (error !== null) {
            commandDebug("Command failed with message '%s'", error.message);
            this.call.write({
              failure: {
                commandId: command.id,
                description: error.message
              }
            });
          } else {

            const msgReply = {
              commandId: command.id,
              sideEffects: effects
            };

            let debugAction;
            if (deleted) {
              debugAction = "that deletes this entity ";
              msgReply.delete = {};
            } else if (this.currentState !== null && noState) {
              debugAction = "that creates this entity ";
              msgReply.create = this.currentState.getStateAndResetDelta();
            } else {
              const delta = this.currentState.getAndResetDelta();
              if (delta != null) {
                debugAction = "that updates this entity ";
                msgReply.update = delta;
              } else {
                debugAction = "";
              }
            }

            if (forward != null) {
              msgReply.forward = forward;
              commandDebug("Sending reply %swith %d side effects and forwarding to '%s.%s'",
                debugAction, msgReply.sideEffects.length, forward.serviceName, forward.commandName);
            } else {
              msgReply.reply = {
                payload: this.entity.serialize(grpcMethod.resolvedResponseType.create(reply))
              };
              commandDebug("Sending reply %swith %d side effects and reply type '%s'",
                debugAction, msgReply.sideEffects.length, msgReply.reply.payload.typeUrl);
            }

            this.call.write({
              reply: msgReply
            });
          }

        } else {
          const msg = "No handler register for command '" + command.name + "'";
          commandDebug(msg);
          this.call.write({
            failure: {
              commandId: command.id,
              description: msg
            }
          })
        }

      } catch (err) {
        const error = "Error handling command '" + command.name + "'";
        commandDebug(error);
        console.error(err);

        this.call.write({
          failure: {
            commandId: command.id,
            description: error + ": " + err
          }
        });

        this.call.end();
      }
    }
  }


  onEnd() {
    this.streamDebug("Stream terminating");
  }

}

class Crdt {

  constructor(desc, serviceName, options) {

    this.options = {
      ...{
        includeDirs: ["."],
      },
      ...options
    };

    const allIncludeDirs = [
      path.resolve(__dirname, "..", "proto"),
      path.resolve(__dirname, "..", "protoc", "include")
    ].concat(this.options.includeDirs);

    this.root = loadProtobuf(desc, allIncludeDirs);

    this.serviceName = serviceName;
    // Eagerly lookup the service to fail early
    this.service = this.root.lookupService(serviceName);

    if (!fs.existsSync("user-function.desc"))
      throw new Error("No 'user-function.desc' file found in application root folder.");

    const packageDefinition = protoLoader.loadSync(desc, {
      includeDirs: allIncludeDirs
    });
    this.grpc = grpc.loadPackageDefinition(packageDefinition);
  }

  entityType() {
    return crdtServices.entityType();
  }

  lookupType(messageType) {
    return this.root.lookupType(messageType);
  }

  setCommandHandlers(handlers) {
    this.commandHandlers = handlers;
  }

  register(allEntities) {
    crdtServices.addService(this, allEntities);
    return crdtServices;
  }

  start(options) {
    const server = new CloudState();
    server.addEntity(this);

    return server.start(options);
  }
}

module.exports = {
  Crdt: Crdt,
  GCounter: GCounter,
  PNCounter: PNCounter,
  GSet: GSet,
  ORSet: ORSet,
  LWWRegister: LWWRegister,
  Flag: Flag,
  Clocks: crdtServices.clocks
};