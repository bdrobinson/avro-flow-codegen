import generate from '@babel/generator';
import * as t from '@babel/types';

import * as a from './avro';
import {
  createFlowTransformer,
  createTypescriptTransformer,
} from './transformers';

const validateAvroCustomName = (string: string) => {
  const regex = /^[A-Za-z_][A-Za-z0-9_]*$/;
  if (regex.test(string) === false) {
    throw new Error(`'${string}' is not a valid avro name`);
  }
};

const createTypeName = (names: ReadonlyArray<string>): string => {
  return names
    .map(name =>
      name
        .replace(' ', '_')
        .split('_')
        .map(capitaliseFirstLetter)
        .join('')
    )
    .map(capitaliseFirstLetter)
    .map(s => s.replace(' ', ''))
    .join('_');
};

const capitaliseFirstLetter = (str: string): string => {
  return str.charAt(0).toUpperCase() + str.slice(1);
};

class Context {
  wrapPrimitives: boolean;
  constructor(wrapPrimitives: boolean) {
    this.wrapPrimitives = wrapPrimitives;
  }
  allTypes: Array<[string, a.AvroType]> = [];

  addType(name: string, newType: a.AvroType) {
    this.allTypes.push([name, newType]);
  }

  isValidCustomType(typeName: string): boolean {
    return new Set(this.allTypes.map(a => a[0])).has(typeName);
  }
}

type Flattener<T extends a.AvroType> = (
  a: T,
  parentNames: ReadonlyArray<string>,
  context: Context,
  nameOverride?: null | string
) => T | a.CustomType;

interface Options {
  wrapPrimitives?: boolean;
  target: 'flow' | 'typescript';
}

export const parseFile = (avscText: string, options: Options): string => {
  const avro: a.AvroType = JSON.parse(avscText);
  const context = new Context(options?.wrapPrimitives ?? true);
  flattenAvroType(avro, [], context);
  switch (options.target) {
    case 'flow': {
      const transformer = createFlowTransformer(context);
      const last = getLast(context.allTypes);
      const file = t.file(
        t.program(
          [
            ...context.allTypes.map(([names, avroType]) =>
              t.exportNamedDeclaration(transformer.declaration(names, avroType))
            ),
            t.exportNamedDeclaration(transformer.alias('__Schema', last[0])),
          ],
          undefined,
          'module',
          null
        ),
        null,
        null
      );
      // lol
      return '// @flow\n\n' + generate(file).code;
    }
    case 'typescript': {
      const transformer = createTypescriptTransformer(context);
      const last = getLast(context.allTypes);
      const file = t.file(
        t.program(
          [
            ...context.allTypes.map(([names, avroType]) =>
              t.exportNamedDeclaration(transformer.declaration(names, avroType))
            ),
            t.exportNamedDeclaration(transformer.alias('__Schema', last[0])),
          ],
          undefined,
          'module',
          null
        ),
        null,
        null
      );
      // lol
      return generate(file).code;
    }
  }
};

const flattenAvroRecord: Flattener<a.AvroRecord> = (
  avro,
  parentNames,
  context,
  nameOverride
) => {
  validateAvroCustomName(avro.name);
  const name = nameOverride ?? avro.name;

  const updated: a.AvroRecord = {
    type: 'record',
    name: avro.name,
    fields: avro.fields.map(field => {
      const updated = flattenAvroType(
        field.type,
        [...parentNames, name, field.name],
        context
      );
      return {
        name: field.name,
        type: updated,
      };
    }),
  };
  context.addType(avro.name, updated);
  return avro.name;
};

const flattenAvroArray: Flattener<a.AvroArray> = (avro, names, context) => {
  return {
    type: 'array',
    items: flattenAvroType(avro.items, names, context),
  };
};

const isAvroTypeNull = (avro: a.AvroType): boolean => {
  return a.handleAvroType<boolean>(avro, {
    primitive: primitive => {
      return primitive === a.AvroPrimitiveType.NULL;
    },
    array: () => false,
    record: () => false,
    union: () => false,
    custom: () => false,
    enum: () => false,
    map: () => false,
  });
};

const flattenAvroEnum: Flattener<a.AvroEnum> = (avro, _, context) => {
  validateAvroCustomName(avro.name);
  context.addType(avro.name, avro);
  return avro.name;
};

const flattenAvroType: Flattener<a.AvroType> = (
  avro: a.AvroType,
  names: ReadonlyArray<string>,
  context: Context
) => {
  return a.handleAvroType<a.AvroType>(avro, {
    primitive: a => a,
    union: a => {
      return flattenAvroUnionType(a, names, context);
    },
    record: a => {
      return flattenAvroRecord(a, [], context);
    },
    array: a => flattenAvroArray(a, names, context),
    custom: a => a,
    enum: a => flattenAvroEnum(a, [], context),
    map: a => flattenAvroMapType(a, names, context),
  });
};

const flattenAvroMapType: Flattener<a.AvroMap> = (avro, names, context) => {
  const typeName = createTypeName([...names, 'Map']);
  const updated: a.AvroMap = {
    type: 'map',
    values: flattenAvroType(avro.values, names, context),
  };
  context.addType(typeName, updated);
  return typeName;
};

const flattenAvroUnionType: Flattener<a.AvroUnion> = (
  unionTypes,
  names,
  context
) => {
  const typeName = names.length > 0 ? createTypeName(names) : 'Union';

  const shouldInline =
    names.length > 0 &&
    unionTypes.length === 2 &&
    unionTypes.some(isAvroTypeNull);

  const updated = unionTypes.map(unionType =>
    flattenAvroType(unionType, names, context)
  );
  if (shouldInline) {
    return updated;
  } else {
    context.addType(typeName, updated);
    return typeName;
  }
};

const getLast = <T>(arr: ReadonlyArray<T>): T => {
  const length = arr.length;
  if (length === 0) {
    throw new Error('Cannot get last of empty array');
  }
  const last = arr[length - 1];
  return last;
};
