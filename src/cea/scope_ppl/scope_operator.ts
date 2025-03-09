import { strict as assert } from "assert";
import { InvalidExpression } from "./invalid_expression";
import { isObjectExpression, scopeExpression } from "./scope_expression";
import _ from "lodash";
import { isOperator } from "./is_operator";

function scopeGetField(
  e: Record<string, unknown>,
  root: string,
  vars: Record<string, true>
) {
  const getField = e["$getField"];
  if (isOperator(getField) || typeof getField === "string") {
    // Shorthand syntax
    return {
      $getField: { $concat: [{ $literal: root }, ".", getField] },
    };
  }
  // Regular syntax, scope as usual
  return _.mapValues(e, (v) => scopeExpression(v, root, vars));
}

function scopeLet(
  e: Record<string, unknown>,
  root: string,
  outerVars: Record<string, true>
): Record<string, unknown> {
  const letValue = e["$let"];
  if (!isObjectExpression(letValue)) {
    throw new InvalidExpression("$let only supports an object as its argument");
  }
  const vars = "vars" in letValue ? letValue["vars"] : undefined;
  if (typeof vars === "undefined") {
    throw new InvalidExpression("Missing 'vars' parameter to $let");
  }
  if (!isObjectExpression(vars)) {
    throw new InvalidExpression("invalid parameter: expected an object (vars)");
  }
  const inn = "in" in letValue ? letValue["in"] : undefined;
  if (typeof inn === "undefined") {
    throw new InvalidExpression("Missing 'in' parameter to $let");
  }
  return {
    $let: {
      vars: scopeExpression(vars, root, outerVars),
      in: scopeExpression(inn, root, {
        ...outerVars,
        ...Object.fromEntries(Object.keys(vars).map((k) => [k, true])),
      }),
    },
  };
}

export function scopeOperator(
  e: Record<string, unknown>,
  root: string,
  vars: Record<string, true>
): Record<string, unknown> {
  const name_ = Object.keys(e)[0];
  assert(name_!.startsWith("$"));
  const name = name_!.substring(1);

  switch (name) {
    case "abs":
    case "acos":
    case "acosh":
    case "add":
    case "addToSet":
    case "allElementsTrue":
    case "and":
    case "anyElementTrue":
    case "arrayElemAt":
    case "arrayToObject":
    case "asin":
    case "asinh":
    case "atan":
    case "atan2":
    case "atanh":
    case "binarySize":
    case "bitAnd":
    case "bitNot":
    case "bitOr":
    case "bitXor":
    case "bsonSize":
    case "ceil":
    case "cmp":
    case "concat":
    case "concatArrays":
    case "cond":
    case "convert":
    case "cos":
    case "cosh":
    case "dateAdd":
    case "dateDiff":
    case "dateFromParts":
    case "dateFromString":
    case "dateSubtract":
    case "dateToParts":
    case "dateToString":
    case "dateTrunc":
    case "dayOfMonth":
    case "dayOfWeek":
    case "dayOfYear":
    case "degreesToRadians":
    case "divide":
    case "eq":
    case "exp":
    case "filter":
    case "floor":
    case "gt":
    case "gte":
    case "hour":
    case "ifNull":
    case "in":
    case "indexOfArray":
    case "indexOfBytes":
    case "indexOfCP":
    case "isArray":
    case "isNumber":
    case "isoDayOfWeek":
    case "isoWeek":
    case "isoWeekYear":
    case "ln":
    case "log":
    case "log10":
    case "lt":
    case "lte":
    case "ltrim":
    case "map":
    case "max":
    case "maxN":
    case "mergeObjects":
    case "min":
    case "minN":
    case "millisecond":
    case "minute":
    case "mod":
    case "month":
    case "multiply":
    case "ne":
    case "not":
    case "objectToArray":
    case "or":
    case "pow":
    case "radiansToDegrees":
    case "rand":
    case "range":
    case "reduce":
    case "regexFind":
    case "regexFindAll":
    case "regexMatch":
    case "replaceOne":
    case "replaceAll":
    case "reverseArray":
    case "round":
    case "rtrim":
    case "sampleRate":
    case "second":
    case "setDifference":
    case "setEquals":
    case "setField":
    case "setIntersection":
    case "setIsSubset":
    case "setUnion":
    case "shift":
    case "size":
    case "sin":
    case "sinh":
    case "slice":
    case "sortArray":
    case "split":
    case "sqrt":
    case "stdDevPop":
    case "stdDevSamp":
    case "strcasecmp":
    case "strLenBytes":
    case "strLenCP":
    case "substr":
    case "substrBytes":
    case "substrCP":
    case "subtract":
    case "sum":
    case "switch":
    case "tan":
    case "tanh":
    case "toBool":
    case "toDate":
    case "toDecimal":
    case "toDouble":
    case "toInt":
    case "toLong":
    case "toObjectId":
    case "toString":
    case "toLower":
    case "toUpper":
    case "toUUID":
    case "tsIncrement":
    case "tsSecond":
    case "trim":
    case "trunc":
    case "type":
    case "unsetField":
    case "week":
    case "year":
    case "zip":
      // recurse into the value
      return _.mapValues(e, (v) => scopeExpression(v, root, vars));
    case "getField":
      return scopeGetField(e, root, vars);
    case "let":
      return scopeLet(e, root, vars);
    case "literal":
      // Don't need to scope literals
      return e;
    default:
      throw new InvalidExpression(`Unrecognized expression: '${name}'`);
  }
}
