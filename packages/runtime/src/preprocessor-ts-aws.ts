import { TSQueryAST, assert } from '@pgtyped/parser';
import { Param, ParamKey, ParamType } from '@pgtyped/parser';
import {
  DictArrayParameter,
  DictParameter,
  // InterpolatedQuery,
  // NestedParameters,
  // QueryParameters,
  ScalarArrayParameter,
  ScalarParameter,
  ParameterTransform,
  QueryParameter,
  replaceIntervals,
  Scalar,
} from './preprocessor.js';

interface Parameter {
  name: string;
  value: {
    stringValue?: string;
    longValue?: number;
    booleanValue?: boolean;
  };
}

export interface NestedParameters {
  [subParamName: string]: Parameter;
}

export interface NestedScalarParameters {
  [subParamName: string]: Scalar;
}

export interface QueryParameters {
  [paramName: string]:
    | Scalar
    | NestedScalarParameters
    | Scalar[]
    | NestedScalarParameters[];
}

export interface InterpolatedQuery {
  query: string;
  mapping: QueryParameter[];
  bindings: Parameter[];
}

function processScalar(
  { name, required }: Param,
  nextIndex: number,
  existingConfig?: ScalarParameter,
  parameters?: QueryParameters,
): {
  replacement: string;
  bindings: Parameter[];
  nextIndex: number;
  config: ScalarParameter;
} {
  let index = nextIndex;
  const bindings = [];
  let replacement;
  let config = existingConfig;
  if (config) {
    replacement = `:param${config.assignedIndex}`;
  } else {
    const assignedIndex = ++index;
    replacement = `:param${assignedIndex}`;
    config = {
      assignedIndex,
      type: ParameterTransform.Scalar,
      name,
      required,
    };

    if (parameters) {
      const value = parameters[name];
      bindings.push(convertToParameter(value, replacement));
    }
  }
  return { bindings, replacement, nextIndex: index, config };
}

function processScalarArray(
  { name, required }: Param,
  nextIndex: number,
  existingConfig?: ScalarArrayParameter,
  parameters?: QueryParameters,
): {
  replacement: string;
  bindings: Parameter[];
  nextIndex: number;
  config: ScalarArrayParameter;
} {
  let index = nextIndex;
  const bindings: Parameter[] = [];
  let config = existingConfig;

  let assignedIndex: number[] = [];
  if (config) {
    assignedIndex = config.assignedIndex as number[];
  } else {
    if (parameters) {
      const values = parameters[name] as any[];
      assignedIndex = values.map((val) => {
        index = index + 1;
        bindings.push(convertToParameter(val, `:param${index}`));
        return index;
      });
    } else {
      assignedIndex = [++index];
    }
    config = {
      assignedIndex,
      type: ParameterTransform.Spread,
      name,
      required,
    };
  }
  const replacement =
    '(' + assignedIndex.map((v) => `:param${v}`).join(', ') + ')';

  return { bindings, replacement, nextIndex: index, config };
}

function processObject(
  paramName: string,
  keys: ParamKey[],
  nextIndex: number,
  existingConfig?: DictParameter,
  parameters?: QueryParameters,
): {
  replacement: string;
  bindings: Parameter[];
  nextIndex: number;
  config: DictParameter;
} {
  let index = nextIndex;
  const bindings: Parameter[] = [];
  const config =
    existingConfig ||
    ({
      name: paramName,
      type: ParameterTransform.Pick,
      dict: {},
    } as DictParameter);

  const keyIndices = keys.map(({ name, required }) => {
    if (name in config.dict) {
      config.dict[name].required = config.dict[name].required || required;
      // reuse index if parameter was seen before
      return `:param${config.dict[name].assignedIndex}`;
    }

    const assignedIndex = ++index;
    const paramIndex = `:param${assignedIndex}`;
    config.dict[name] = {
      assignedIndex,
      name,
      required,
      type: ParameterTransform.Scalar,
    };
    if (parameters) {
      const value = (parameters[paramName] as NestedScalarParameters)[name];
      bindings.push(convertToParameter(value, paramIndex));
    }
    return paramIndex;
  });
  const replacement = '(' + keyIndices.join(', ') + ')';

  return { bindings, replacement, nextIndex: index, config };
}

function processObjectArray(
  paramName: string,
  keys: ParamKey[],
  nextIndex: number,
  existingConfig?: DictArrayParameter,
  parameters?: QueryParameters,
): {
  replacement: string;
  bindings: Parameter[];
  nextIndex: number;
  config: DictArrayParameter;
} {
  let index = nextIndex;
  const bindings: Parameter[] = [];
  const config =
    existingConfig ||
    ({
      name: paramName,
      type: ParameterTransform.PickSpread,
      dict: {},
    } as DictArrayParameter);

  let replacement;
  if (parameters) {
    const values = parameters[paramName] as NestedScalarParameters[];
    if (values.length > 0) {
      replacement = values
        .map((val) =>
          keys
            .map(({ name }) => {
              const paramIndex = `:param${++index}`;
              bindings.push(convertToParameter(val[name], paramIndex));
              return paramIndex;
            })
            .join(', '),
        )
        .map((pk) => `(${pk})`)
        .join(', ');
    } else {
      // empty set for empty arrays
      replacement = '()';
    }
  } else {
    const keyIndices = keys.map(({ name, required }) => {
      if (name in config.dict) {
        // reuse index if parameter was seen before
        return `:param${config.dict[name].assignedIndex}`;
      }

      const assignedIndex = ++index;
      config.dict[name] = {
        assignedIndex,
        name,
        required,
        type: ParameterTransform.Scalar,
      };
      return `:param${assignedIndex}`;
    });
    replacement = '(' + keyIndices.join(', ') + ')';
  }

  return { bindings, replacement, nextIndex: index, config };
}

/* Processes query strings produced by old parser from SQL-in-TS statements */
export const processTSQueryASTForAWS = (
  query: TSQueryAST,
  parameters?: QueryParameters,
): InterpolatedQuery => {
  const bindings: Parameter[] = [];
  const baseMap: { [param: string]: QueryParameter } = {};
  let i = 0;
  const intervals: { a: number; b: number; sub: string }[] = [];
  for (const param of query.params) {
    let sub: string;
    let paramBindings: Parameter[] = [];
    let config: QueryParameter;
    let result;
    if (param.selection.type === ParamType.Scalar) {
      const prevConfig = baseMap[param.name] as ScalarParameter | undefined;
      result = processScalar(param, i, prevConfig, parameters);
    }
    if (param.selection.type === ParamType.ScalarArray) {
      const prevConfig = baseMap[param.name] as
        | ScalarArrayParameter
        | undefined;
      result = processScalarArray(param, i, prevConfig, parameters);
    }
    if (param.selection.type === ParamType.Object) {
      const prevConfig: DictParameter = (baseMap[
        param.name
      ] as DictParameter) || {
        name: param.name,
        type: ParameterTransform.Pick,
        dict: {},
      };
      result = processObject(
        param.name,
        param.selection.keys,
        i,
        prevConfig,
        parameters,
      );
    }
    if (param.selection.type === ParamType.ObjectArray) {
      const prevConfig: DictArrayParameter = (baseMap[
        param.name
      ] as DictArrayParameter) || {
        name: param.name,
        type: ParameterTransform.PickSpread,
        dict: {},
      };
      result = processObjectArray(
        param.name,
        param.selection.keys,
        i,
        prevConfig,
        parameters,
      );
    }
    assert(result);
    ({
      config,
      nextIndex: i,
      replacement: sub,
      bindings: paramBindings,
    } = result);
    baseMap[param.name] = config!;
    bindings.push(...paramBindings);
    intervals.push({ a: param.location.a, b: param.location.b, sub });
  }
  const flatStr = replaceIntervals(query.text, intervals);
  return {
    mapping: parameters ? [] : Object.values(baseMap),
    query: flatStr,
    bindings,
  };
};

function convertToParameter(value: any, pramIndex: string): Parameter {
  // remove first character which is a colon
  const name = pramIndex.slice(1);
  if (typeof value === 'number') {
    return { name, value: { longValue: value } };
  } else if (typeof value === 'string') {
    return { name, value: { stringValue: value } };
  } else if (typeof value === 'boolean') {
    return { name, value: { booleanValue: value } };
  } else {
    return { name, value: { stringValue: JSON.stringify(value) } };
  }
}
