const Twitter = require("twitter");

const OrientDB = require("orientjs").OrientDBClient;

const consumer_key = process.env.TWITTER_CONSUMER_KEY;
const consumer_secret = process.env.TWITTER_CONSUMER_SECRET;
const access_token_key = process.env.TWITTER_TOKEN_KEY;
const access_token_secret = process.env.TWITTER_TOKEN_SECRET;

const orientdb_host = process.env.ORIENTDB_HOST || "localhost";
const orientdb_port = process.env.ORIENTDB_PORT
  ? parseInt(process.env.ORIENTDB_PORT)
  : 2424;

const database = process.env.ORIENTDB_DATABASE || "twitter_stream";

const rootUser = process.env.ORIENTDB_ROOT_USER || "root";
const rootPassword = process.env.ORIENTDB_ROOT_PASSWORD || "root";

const databaseUser = process.env.ORIENTDB_DB_USER || "admin";
const databasePassword = process.env.ORIENTDB_DB_PASSWORD || "admin";

const startup = async () => {
  let orientdb = await OrientDB.connect(orientdb_host, orientdb_port);

  let exists = await orientdb.existsDatabase({
    name: database,
    username: rootUser,
    password: rootPassword
  });
  if (!exists) {
    await orientdb.createDatabase({
      name: database,
      username: rootUser,
      password: rootPassword
    });

    let session = await orientdb.session({
      name: database,
      username: databaseUser,
      password: databasePassword
    });

    await session.command("CREATE CLASS User extends V").all();
    await session.command("CREATE PROPERTY User.id LONG").all();
    await session.command("CREATE INDEX User.id ON User (id) UNIQUE").all();

    await session.command("CREATE CLASS Tweet extends V").all();
    await session.command("CREATE PROPERTY Tweet.id LONG").all();
    await session.command("CREATE INDEX Tweet.id ON Tweet (id) UNIQUE").all();

    await session.command("CREATE CLASS Tag extends V").all();
    await session.command("CREATE PROPERTY Tag.label STRING").all();
    await session.command("CREATE INDEX Tag.label ON Tag (label) UNIQUE").all();

    await session.command("CREATE CLASS HasTweet extends E").all();
    await session.command("CREATE CLASS HasTag extends E").all();

    await session.close();
  }

  let pool = await orientdb.sessions({
    name: database,
    username: databaseUser,
    password: databasePassword
  });

  let client = new Twitter({
    consumer_key,
    consumer_secret,
    access_token_key,
    access_token_secret
  });

  let stream = client.stream("statuses/filter", { track: "javascript" });
  stream.on("data", async event => {
    let session = await pool.acquire();

    try {
      await session.runInTransaction(async tx => {
        let tweet = await tx
          .command("create vertex Tweet set id = :id", {
            params: { id: event.id }
          })
          .one();

        let user = await tx
          .command(
            "UPDATE User set id = :id UPSERT RETURN AFTER where id = :id ",
            {
              params: { id: event.user.id }
            }
          )
          .one();

        await tx
          .command("CREATE EDGE HasTweet FROM :from TO :to", {
            params: {
              from: user["@rid"],
              to: tweet["@rid"]
            }
          })
          .one();

        let tags = await Promise.all(
          event.entities.hashtags.map(ht => {
            return tx
              .command(
                "UPDATE Tag set label = :label UPSERT RETURN AFTER where label = :label",
                {
                  params: {
                    label: ht.text
                  }
                }
              )
              .one();
          })
        );

        await Promise.all(
          tags.map(t => {
            return tx
              .command("CREATE EDGE HasTag FROM :from TO :to", {
                params: {
                  from: tweet["@rid"],
                  to: t["@rid"]
                }
              })
              .one();
          })
        );
      });
    } catch (e) {
      console.log(e);
    }

    await session.close();
  });

  stream.on("error", function(error) {
    console.log(error);
  });
};

startup();
