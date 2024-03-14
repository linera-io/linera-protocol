function loopFiles(item, callback) {
  var dirReader = item.createReader();
  var fileList = [];
  function sequence() {
    dirReader.readEntries(function (entries) {
      var entryList = Array.prototype.slice.apply(entries);
      fileList = fileList.concat(entryList);

      // Check if all the file has been viewed
      var isFinished = !entryList.length;
      if (isFinished) {
        callback(fileList);
      } else {
        sequence();
      }
    });
  }
  sequence();
}
var traverseFileTree = function traverseFileTree(files, callback, isAccepted) {
  // eslint-disable-next-line @typescript-eslint/naming-convention
  var _traverseFileTree = function _traverseFileTree(item, path) {
    if (!item) {
      return;
    }
    // eslint-disable-next-line no-param-reassign
    item.path = path || '';
    if (item.isFile) {
      item.file(function (file) {
        if (isAccepted(file)) {
          // https://github.com/ant-design/ant-design/issues/16426
          if (item.fullPath && !file.webkitRelativePath) {
            Object.defineProperties(file, {
              webkitRelativePath: {
                writable: true
              }
            });
            // eslint-disable-next-line no-param-reassign
            file.webkitRelativePath = item.fullPath.replace(/^\//, '');
            Object.defineProperties(file, {
              webkitRelativePath: {
                writable: false
              }
            });
          }
          callback([file]);
        }
      });
    } else if (item.isDirectory) {
      loopFiles(item, function (entries) {
        entries.forEach(function (entryItem) {
          _traverseFileTree(entryItem, "".concat(path).concat(item.name, "/"));
        });
      });
    }
  };
  files.forEach(function (file) {
    _traverseFileTree(file.webkitGetAsEntry());
  });
};
export default traverseFileTree;