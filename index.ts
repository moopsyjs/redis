import { MoopsyServer } from "@moopsyjs/server";
import * as EJSON from 'ejson';
import { RedisClientOptions, createClient } from "redis";

type OptionsType = {
    url: string,
} | {
    redisClientOptions: RedisClientOptions
};

type RedisMessageType =
    {type:"publish-to-topic" | "publish-internal", originatorServer:string, data:{ topic:string, message:any } }
    | { type: "pubsub::publish", originatorServer: string, data: Array<{ topic: string, message: any }> }
;

const REDIS_KEY = "seamless-cross-server-channel";

async function connectServer (server: MoopsyServer<any, any>, options: OptionsType) {
    await new Promise<void>(async (resolve, reject) => {
        const pub = createClient('redisClientOptions' in options ? options.redisClientOptions : {
            url: options.url,
        });
        const sub = createClient('redisClientOptions' in options ? options.redisClientOptions : {
            url: options.url,
        });

        pub.on('error', reject);
        sub.on('error', reject);

        await pub.connect();
        await sub.connect();        

        const serverId = server.serverId;

        await sub.subscribe(REDIS_KEY, (rawMessage: string) => {
            const message: RedisMessageType = EJSON.parse(rawMessage);

            if(message.originatorServer !== serverId) {
                if(message.type === "publish-to-topic") {
                    server.topics.publish(message.data.topic, message.data.message, true);
                }
                if(message.type === "pubsub::publish") {
                    for(const event of message.data) {
                        server.topics.publish(event.topic, event.message, true);
                    }
                }
                if(message.type === "publish-internal" && message.originatorServer !== serverId) {
                    server.__iv.emit("publish-internal", message.data);
                }
            }
        })

        server.__iv.on("publish-to-topic", event => {
            const message: RedisMessageType = {
                type:"publish-to-topic",
                originatorServer: serverId,
                data: event,
            }
            pub.publish(REDIS_KEY, EJSON.stringify(message));
        });
        server.__iv.on("pubsub::publish", event => {
            const message: RedisMessageType = {
                type:"publish-to-topic",
                originatorServer: serverId,
                data: event,
            }
            pub.publish(REDIS_KEY, EJSON.stringify(message));
        });        
        server.__iv.on("publish-internal", event => {
            const message: RedisMessageType = {
                type:"publish-internal",
                originatorServer: serverId,
                data: event,
            }
            pub.publish(REDIS_KEY, EJSON.stringify(message));
        });
        resolve();
    });
}

export default {
    protocolVersion:"1",
    connectServer,
};