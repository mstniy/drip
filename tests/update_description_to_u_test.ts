import { describe, it } from "node:test";
import { updateDescriptionToU } from "../src/persister/update_description_to_u";
import { strict as assert } from "assert";

describe("updateDescriptionToU", () => {
  it("works", () => {
    const res = updateDescriptionToU({
      disambiguatedPaths: {
        "a.b": ["a.b"],
        "a.c": ["a", "c"],
        "b.0": ["b", 0],
      },
      removedFields: ["a.b", "c"],
      truncatedArrays: [
        {
          field: "a.d",
          newSize: 5,
        },
        {
          field: "b.1",
          newSize: 3,
        },
      ],
      updatedFields: {
        d: 1,
        "b.0": "hey",
        "b.2": "there",
      },
    });

    assert.deepStrictEqual(res, {
      d: {
        "a.b": false,
        c: false,
      },
      sa: {
        t: {
          d: 5,
        },
      },
      sb: {
        i: {
          "0": "hey",
          "2": "there",
        },
        t: {
          "1": 3,
        },
      },
      i: {
        d: 1,
      },
    });
  });
});
