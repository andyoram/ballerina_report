import ballerina/http;
import ballerina/log;

type Product record {
    int id;
    string name;
    float price;
};

type Order record {
    int id?;
    float total?;
    boolean processed = false;
    Product[] products?;
};

@http:ServiceConfig {
    basePath: "/StoreService"
}
service StoreService on new http:Listener(9090) {
    @http:ResourceConfig {
        methods: ["GET"],
        path: "/processOrder"
    }
    resource function processOrder(http:Caller outboundEP, http:Request req) returns error? {
        map<any> qParams = req.getQueryParams();
        int | error orderId = int.convert(qParams["orderId"]);
        if (orderId is int && orderId > 0) {
            Order | error retrievedOrder = getOrder(untaint orderId);
            if (retrievedOrder is error) {
                log:printError("error in retrieving order details.", err = retrievedOrder);
                respond(outboundEP, "error in retrieving order details.", statusCode = 500);
            } else {
                json jsonResponse = check json.convert(retrievedOrder);
                respond(outboundEP, untaint jsonResponse);
            }
        } else {
            log:printError("invalid input query parameter. expected a positive integer.");
            respond(outboundEP, "invalid input query parameter. expected a positive integer.", statusCode = 400);
        }
    }
}

http:Client clientEP = new ("http://localhost:9091");

function getOrder(int orderId) returns Order | error {
    var response = clientEP->get("/OrderService/getOrder?orderId=" + orderId);
    if (response is http:Response) {
        var payload = check response.getJsonPayload();
        var productOrder = Order.stamp(payload);
        if (productOrder is Order) {
            productOrder.processed = true;
            return productOrder;
        } else {
            log:printError("data received in invalid.", err = productOrder);
        }
    } else {
        log:printError("failed to retrieve order infomation.", err = response);
    }
    error e = error("failed to retrieve order infomation.");
    return e;
}

function respond(http:Caller outboundEP, json | string payload, int statusCode = 200) {
    http:Response res = new;
    res.statusCode = statusCode;
    res.setJsonPayload(payload, contentType = "application/json");
    error? responseStatus = outboundEP->respond(res);
    if (responseStatus is error) {
        log:printError("error in sending response.", err = responseStatus);
    }
}
