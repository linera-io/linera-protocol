var hyphen2camel = function (str) {
  return str
          .toLowerCase()
          .replace(/-[a-z]/g, function (match) {
             return match.slice(1).toUpperCase() ;
          });
};

module.exports = hyphen2camel;