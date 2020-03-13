import prettier from 'prettier';

import { parseFile } from '../src';

const format = (file: string): string => {
  return prettier.format(file, { parser: 'babel' });
};

const assert = (schema: {}, expectedFile: string) => {
  const parsed = parseFile(JSON.stringify(schema));
  expect(format(parsed)).toBe(format(expectedFile));
};

test('simple schema works', () => {
  const schema = {
    type: 'record',
    name: 'Person',
    fields: [
      { name: 'age', type: 'int' },
      {
        name: 'friends',
        type: {
          type: 'array',
          items: 'string',
        },
      },
      { name: 'spouse_name', type: ['null', 'string'] },
      {
        name: 'favourite_pet',
        type: {
          type: 'record',
          name: 'Pet',
          fields: [{ name: 'species', type: 'string' }],
        },
      },
    ],
  };
  const expected = `
  // @flow

  export type Person_Spousename = null | {|
    string: string
    |}
    export type Pet = {|
      species: string
    |}
    export type Person = {|
        age: number,
        friends: string[],
        spouse_name: Person_Spousename,
        favourite_pet: Pet
    |}
  `;
  assert(schema, expected);
});
