"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");
var _typeof = require("@babel/runtime/helpers/typeof");
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;
var _toConsumableArray2 = _interopRequireDefault(require("@babel/runtime/helpers/toConsumableArray"));
var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));
var _KeyCode = _interopRequireDefault(require("rc-util/lib/KeyCode"));
var React = _interopRequireWildcard(require("react"));
var _useSearchOptions = require("../hooks/useSearchOptions");
var _commonUtil = require("../utils/commonUtil");
function _getRequireWildcardCache(e) { if ("function" != typeof WeakMap) return null; var r = new WeakMap(), t = new WeakMap(); return (_getRequireWildcardCache = function _getRequireWildcardCache(e) { return e ? t : r; })(e); }
function _interopRequireWildcard(e, r) { if (!r && e && e.__esModule) return e; if (null === e || "object" != _typeof(e) && "function" != typeof e) return { default: e }; var t = _getRequireWildcardCache(r); if (t && t.has(e)) return t.get(e); var n = { __proto__: null }, a = Object.defineProperty && Object.getOwnPropertyDescriptor; for (var u in e) if ("default" !== u && Object.prototype.hasOwnProperty.call(e, u)) { var i = a ? Object.getOwnPropertyDescriptor(e, u) : null; i && (i.get || i.set) ? Object.defineProperty(n, u, i) : n[u] = e[u]; } return n.default = e, t && t.set(e, n), n; }
var _default = exports.default = function _default(ref, options, fieldNames, activeValueCells, setActiveValueCells, onKeyBoardSelect, contextProps) {
  var direction = contextProps.direction,
    searchValue = contextProps.searchValue,
    toggleOpen = contextProps.toggleOpen,
    open = contextProps.open;
  var rtl = direction === 'rtl';
  var _React$useMemo = React.useMemo(function () {
      var activeIndex = -1;
      var currentOptions = options;
      var mergedActiveIndexes = [];
      var mergedActiveValueCells = [];
      var len = activeValueCells.length;
      var pathKeys = (0, _commonUtil.getFullPathKeys)(options, fieldNames);

      // Fill validate active value cells and index
      var _loop = function _loop(i) {
        // Mark the active index for current options
        var nextActiveIndex = currentOptions.findIndex(function (option, index) {
          return (pathKeys[index] ? (0, _commonUtil.toPathKey)(pathKeys[index]) : option[fieldNames.value]) === activeValueCells[i];
        });
        if (nextActiveIndex === -1) {
          return 1; // break
        }
        activeIndex = nextActiveIndex;
        mergedActiveIndexes.push(activeIndex);
        mergedActiveValueCells.push(activeValueCells[i]);
        currentOptions = currentOptions[activeIndex][fieldNames.children];
      };
      for (var i = 0; i < len && currentOptions; i += 1) {
        if (_loop(i)) break;
      }

      // Fill last active options
      var activeOptions = options;
      for (var _i = 0; _i < mergedActiveIndexes.length - 1; _i += 1) {
        activeOptions = activeOptions[mergedActiveIndexes[_i]][fieldNames.children];
      }
      return [mergedActiveValueCells, activeIndex, activeOptions, pathKeys];
    }, [activeValueCells, fieldNames, options]),
    _React$useMemo2 = (0, _slicedToArray2.default)(_React$useMemo, 4),
    validActiveValueCells = _React$useMemo2[0],
    lastActiveIndex = _React$useMemo2[1],
    lastActiveOptions = _React$useMemo2[2],
    fullPathKeys = _React$useMemo2[3];

  // Update active value cells and scroll to target element
  var internalSetActiveValueCells = function internalSetActiveValueCells(next) {
    setActiveValueCells(next);
  };

  // Same options offset
  var offsetActiveOption = function offsetActiveOption(offset) {
    var len = lastActiveOptions.length;
    var currentIndex = lastActiveIndex;
    if (currentIndex === -1 && offset < 0) {
      currentIndex = len;
    }
    for (var i = 0; i < len; i += 1) {
      currentIndex = (currentIndex + offset + len) % len;
      var _option = lastActiveOptions[currentIndex];
      if (_option && !_option.disabled) {
        var nextActiveCells = validActiveValueCells.slice(0, -1).concat(fullPathKeys[currentIndex] ? (0, _commonUtil.toPathKey)(fullPathKeys[currentIndex]) : _option[fieldNames.value]);
        internalSetActiveValueCells(nextActiveCells);
        return;
      }
    }
  };

  // Different options offset
  var prevColumn = function prevColumn() {
    if (validActiveValueCells.length > 1) {
      var nextActiveCells = validActiveValueCells.slice(0, -1);
      internalSetActiveValueCells(nextActiveCells);
    } else {
      toggleOpen(false);
    }
  };
  var nextColumn = function nextColumn() {
    var _lastActiveOptions$la;
    var nextOptions = ((_lastActiveOptions$la = lastActiveOptions[lastActiveIndex]) === null || _lastActiveOptions$la === void 0 ? void 0 : _lastActiveOptions$la[fieldNames.children]) || [];
    var nextOption = nextOptions.find(function (option) {
      return !option.disabled;
    });
    if (nextOption) {
      var nextActiveCells = [].concat((0, _toConsumableArray2.default)(validActiveValueCells), [nextOption[fieldNames.value]]);
      internalSetActiveValueCells(nextActiveCells);
    }
  };
  React.useImperativeHandle(ref, function () {
    return {
      // scrollTo: treeRef.current?.scrollTo,
      onKeyDown: function onKeyDown(event) {
        var which = event.which;
        switch (which) {
          // >>> Arrow keys
          case _KeyCode.default.UP:
          case _KeyCode.default.DOWN:
            {
              var offset = 0;
              if (which === _KeyCode.default.UP) {
                offset = -1;
              } else if (which === _KeyCode.default.DOWN) {
                offset = 1;
              }
              if (offset !== 0) {
                offsetActiveOption(offset);
              }
              break;
            }
          case _KeyCode.default.LEFT:
            {
              if (searchValue) {
                break;
              }
              if (rtl) {
                nextColumn();
              } else {
                prevColumn();
              }
              break;
            }
          case _KeyCode.default.RIGHT:
            {
              if (searchValue) {
                break;
              }
              if (rtl) {
                prevColumn();
              } else {
                nextColumn();
              }
              break;
            }
          case _KeyCode.default.BACKSPACE:
            {
              if (!searchValue) {
                prevColumn();
              }
              break;
            }

          // >>> Select
          case _KeyCode.default.ENTER:
            {
              if (validActiveValueCells.length) {
                var _option2 = lastActiveOptions[lastActiveIndex];

                // Search option should revert back of origin options
                var originOptions = (_option2 === null || _option2 === void 0 ? void 0 : _option2[_useSearchOptions.SEARCH_MARK]) || [];
                if (originOptions.length) {
                  onKeyBoardSelect(originOptions.map(function (opt) {
                    return opt[fieldNames.value];
                  }), originOptions[originOptions.length - 1]);
                } else {
                  onKeyBoardSelect(validActiveValueCells, lastActiveOptions[lastActiveIndex]);
                }
              }
              break;
            }

          // >>> Close
          case _KeyCode.default.ESC:
            {
              toggleOpen(false);
              if (open) {
                event.stopPropagation();
              }
            }
        }
      },
      onKeyUp: function onKeyUp() {}
    };
  });
};