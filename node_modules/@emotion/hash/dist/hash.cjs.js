'use strict';

if (process.env.NODE_ENV === "production") {
  module.exports = require("./hash.cjs.prod.js");
} else {
  module.exports = require("./hash.cjs.dev.js");
}
