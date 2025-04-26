import {
  ChangeStream,
  ChangeStreamDocument,
  Document,
  ResumeToken,
  Timestamp,
} from "mongodb";
import { decodeResumeToken } from "mongodb-resumetoken-decoder";
import z from "zod";

type NoopyCSNothing = { type: "nothing" };
type NoopyCSNoop = { type: "noop"; rt: ResumeToken; ct: Timestamp };

type NoopyCSEvent<TChange extends Document> =
  | { type: "change"; change: TChange }
  | NoopyCSNoop
  | NoopyCSNothing;

export async function* noopyCS<TLocal extends Document>(
  cs: ChangeStream<TLocal, ChangeStreamDocument<TLocal>>
): AsyncGenerator<NoopyCSEvent<ChangeStreamDocument<TLocal>>, void, void> {
  try {
    let noop: NoopyCSNoop | undefined;
    let lastRTData: string | undefined;
    // We are interested in recording noops, but the change
    // stream filters them out. So we listen explicitly
    // for resumeTokenChanged.
    cs.on("resumeTokenChanged", (resumeToken) => {
      const newResumeTokenData = z
        .object({ _data: z.string() })
        .parse(resumeToken)._data;

      // The MongoDB Node driver emits this event
      // after each empty batch, even if the
      // resume token did not actually change.
      // So we do our own deduplication.
      if (newResumeTokenData !== lastRTData) {
        lastRTData = newResumeTokenData;
        const decoded = decodeResumeToken(newResumeTokenData);

        noop = {
          type: "noop",
          rt: resumeToken,
          // mongodb-resumetoken-decoder and the actual driver use
          // incompatible bson versions, so translate between
          // the two
          ct: Timestamp.fromBits(decoded.timestamp.low, decoded.timestamp.high),
        } satisfies NoopyCSNoop;
      }
    });

    while (true) {
      // Use tryNext instead of next to make the
      // query return if there are still no events
      // after maxAwaitTimeMS.
      // We use this to avoid getting stuck on a
      // single next(), as we do want to yield
      // periodically.
      const ce = await cs.tryNext();

      if (ce) {
        yield { type: "change", change: ce };
      } else if (noop) {
        // If there is no change event, but nevertheless
        // a more recent resume token, emit it as a noop
        yield noop;
      } else {
        // This ensures we yield once per tryNext() call
        yield { type: "nothing" };
      }
      noop = undefined;
    }
  } finally {
    await cs.close();
  }
}
