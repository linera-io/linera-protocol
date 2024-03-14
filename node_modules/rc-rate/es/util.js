function getScroll(w) {
  var ret = w.pageXOffset;
  var method = 'scrollLeft';
  if (typeof ret !== 'number') {
    var d = w.document;
    // ie6,7,8 standard mode
    ret = d.documentElement[method];
    if (typeof ret !== 'number') {
      // quirks mode
      ret = d.body[method];
    }
  }
  return ret;
}
function getClientPosition(elem) {
  var x;
  var y;
  var doc = elem.ownerDocument;
  var body = doc.body;
  var docElem = doc && doc.documentElement;
  var box = elem.getBoundingClientRect();
  x = box.left;
  y = box.top;
  x -= docElem.clientLeft || body.clientLeft || 0;
  y -= docElem.clientTop || body.clientTop || 0;
  return {
    left: x,
    top: y
  };
}
export function getOffsetLeft(el) {
  var pos = getClientPosition(el);
  var doc = el.ownerDocument;
  // Only IE use `parentWindow`
  var w = doc.defaultView || doc.parentWindow;
  pos.left += getScroll(w);
  return pos.left;
}