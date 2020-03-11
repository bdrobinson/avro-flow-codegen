import prettier from 'prettier';
import fs from 'fs';

import { generateFile } from '../src';

const format = (text: string) => prettier.format(text, { parser: 'babel' });

describe('blah', () => {
  it('works', () => {
    const json = fs
      .readFileSync('./test/supplier-orders-events-v2.avsc')
      .toString('utf-8');
    const result = generateFile('MyType', json);
    expect(format(result)).toBe(
      format(`
      type MyType = {|
        id: string,
        event: {|
          SupplierOrderConfirmed: {|
              supplier_order_id: string,
              confirmed_by: string,
              confirmed_at: number,
            |},
          |}| {|
            NewSupplierOrder: {|
              thing1: string,
              thing2: null | {|
                string: string,
                |},
            |}
            |}
      |}
    `)
    );
  });
});
