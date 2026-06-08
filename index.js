import express from "express";
const app = express();
app.use(express.json());

const SECRET = process.env.KEEPALIVE_SECRET || "";

function checkSecret(req, res, next) {
  if (!SECRET) return next();
  if (req.get("x-keepalive-secret") !== SECRET) {
    return res.status(403).json({ ok: false, error: "forbidden" });
  }
  next();
}

app.get("/", (_, res) => res.send("ok"));
app.get("/ping", checkSecret, (_, res) => res.json({ ok: true, time: Date.now() }));
app.post("/start", checkSecret, async (req, res) => res.json({ ok: true, action: "start", body: req.body || {} }));
app.post("/stop", checkSecret, async (req, res) => res.json({ ok: true, action: "stop" }));

const port = process.env.PORT || 10000;
app.listen(port, "0.0.0.0", () => console.log(`listening on ${port}`));