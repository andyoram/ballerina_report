import ballerina/http;
import ballerina/log;

type Product record {|
    int id;
    string name;
    float price;
|};

type Order record {|
    int id;
    float total;
    boolean processed = false;
    Product[] products?;
|};

type Inventory record {|
    int productId;
    int stock;
|};

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
            json | error retrievedOrder = getOrder(untaint orderId);
            if (retrievedOrder is error) {
                log:printError("error in retrieving order details.", err = retrievedOrder);
                respond(outboundEP, "error in retrieving order details.", statusCode = 500);
            } else {
                respond(outboundEP, untaint retrievedOrder);
            }
        } else {
            log:printError("invalid input query parameter. expected a positive integer.");
            respond(outboundEP, "invalid input query parameter. expected a positive integer.", statusCode = 400);
        }
    }
}

http:Client clientEP = new ("http://localhost:9091");

function getOrder(int orderId) returns json | error {
    var response = clientEP->get("/OrderService/getOrder?orderId=" + orderId);
    if (response is http:Response) {
        json payload = check response.getJsonPayload();
        var productOrder = Order.stamp(payload.orderDetails);
        var productInventory = Inventory[].stamp(payload.inventoryDetails);
        if (productOrder is error) {
            log:printError("order data received in invalid.", err = productOrder);
        }
        if (productInventory is error) {
            log:printError("inventory data received in invalid.", err = productInventory);
        }
        if (productOrder is Order && productInventory is Inventory[]) {
            productOrder.processed = true;
            json finalPayload = { orderDetails: check json.convert(productOrder), inventoryDetails: check json.convert(productInventory) };
            return finalPayload;
        }

    } else {
        log:printError("failed to retrieve order infomation.", err = response);
    }
    error e = error("failed to retrieve order information.");
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
