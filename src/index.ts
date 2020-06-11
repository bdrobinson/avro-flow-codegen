import generate from '@babel/generator';
import * as t from '@babel/types';

import * as a from './avro';
import * as transformers from './transformers';

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

type Visitor<T extends a.AvroType> = (
  a: T,
  parentNames: ReadonlyArray<string>,
  context: Context,
  nameOverride?: null | string
) => T | a.CustomType;

interface Options {
  wrapPrimitives: boolean;
}

export const parseFile = (avscText: string, options?: Options): string => {
  const avro: a.AvroType = JSON.parse(avscText);
  const context = new Context(options?.wrapPrimitives ?? true);
  visitAvroType(avro, [], context);
  const file = t.file(
    t.program(
      context.allTypes.map(([names, avroType]) =>
        t.exportNamedDeclaration(
          t.typeAlias(
            t.identifier(names),
            null,
            transformers.parseAvroType(avroType, context)
          )
        )
      ),
      undefined,
      'module',
      null
    ),
    null,
    null
  );
  // lol
  return '// @flow\n\n' + generate(file).code;
};

const visitAvroRecord: Visitor<a.AvroRecord> = (
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
      const updated = visitAvroType(
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

const visitAvroArray: Visitor<a.AvroArray> = (avro, names, context) => {
  return {
    type: 'array',
    items: visitAvroType(avro.items, names, context),
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

const visitAvroEnum: Visitor<a.AvroEnum> = (avro, _, context) => {
  validateAvroCustomName(avro.name);
  context.addType(avro.name, avro);
  return avro.name;
};

const visitAvroType: Visitor<a.AvroType> = (
  avro: a.AvroType,
  names: ReadonlyArray<string>,
  context: Context
) => {
  return a.handleAvroType<a.AvroType>(avro, {
    primitive: a => a,
    union: a => {
      return visitAvroUnionType(a, names, context);
    },
    record: a => {
      return visitAvroRecord(a, [], context);
    },
    array: a => visitAvroArray(a, names, context),
    custom: a => a,
    enum: a => visitAvroEnum(a, [], context),
    map: a => visitAvroMapType(a, names, context),
  });
};

const visitAvroMapType: Visitor<a.AvroMap> = (avro, names, context) => {
  const typeName = createTypeName([...names, 'Map']);
  const updated: a.AvroMap = {
    type: 'map',
    values: visitAvroType(avro.values, names, context),
  };
  context.addType(typeName, updated);
  return typeName;
};

const visitAvroUnionType: Visitor<a.AvroUnion> = (
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
    visitAvroType(unionType, names, context)
  );
  if (shouldInline) {
    return updated;
  } else {
    context.addType(typeName, updated);
    return typeName;
  }
};
