import express from "express";
const app=express();
app.use(express.json());
app.get("/",(_,res)=>res.send("ok"));
app.get("/ping",(_,res)=>res.json({ok:true,time:Date.now()}));
app.post("/start",async(req,res)=>res.json({ok:true,action:"start",body:req.body||{}}));
app.post("/stop",async(req,res)=>res.json({ok:true,action:"stop"}));
const port=process.env.PORT||10000;
app.listen(port,"0.0.0.0",()=>console.log(`listening on ${port}`));