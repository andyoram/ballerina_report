import ballerina/http;
import ballerina/log;
import ballerina/mysql;
import ballerina/sql;

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

type OrderEntry record {
    int productId;
};

@http:ServiceConfig {
    basePath: "/OrderService"
}
service OrderService on new http:Listener(9091) {
    @http:ResourceConfig {
        methods: ["GET"],
        path: "/getOrder"
    }
    resource function getOrder(http:Caller outboundEP, http:Request req) returns error? {
        map<any> qParams = req.getQueryParams();
        int|error orderId = int.convert(qParams["orderId"]);
        if (orderId is int && orderId > 0) {
            var productOrder = getProductsForOrder(orderId);
            if (productOrder is error) {
                log:printError("error in retrieving product information.", err = productOrder);
                respond(outboundEP, "error in retrieving product information.", statusCode = 500);
            } else {
                json payload = check json.convert(productOrder);
                respond(outboundEP, untaint payload);
            }
        } else {
            log:printError("invalid input query parameter. expected a positive integer.");
            respond(outboundEP, "invalid input query parameter. expected a positive integer.", statusCode = 400);
        }
    }
}

mysql:Client dbClient = new({
    host: "localhost",
    port: 3306,
    name: "testdb",
    username: "root",
    password: "root",
    poolOptions: { maximumPoolSize: 5 },
    dbOptions: { useSSL: false }
});

http:Client clientEP = new ("http://localhost:9092");

function getProductsForOrder(int id) returns Order | error {
    float total = 0;
    int count = 0;
    Product[] vProducts = [];
    sql:Parameter param = {
        sqlType: sql:TYPE_INTEGER,
        value: id
    };
    var result = dbClient->select("SELECT productId FROM ORDERS WHERE orderId = ?", OrderEntry, param);
    if (result is error) {
        return result;
    } else {
        foreach var row in result {
            http:Request req = new;
            var productId = check row.productId;
            int pId = check sanitizeInt(productId);
            var result2 = check clientEP->get("/ProductService/getProduct?productId=" + pId);
            var payload = result2.getJsonPayload();
            Product product = check Product.convert(payload);
            vProducts[count] = product;
            count = count + 1;
            total = total + product.price;
        }
    }
    Order productOrder = { };
    productOrder.id = id;
    productOrder.total = total;
    productOrder.products = vProducts;

    return productOrder;
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

function sanitizeInt(anydata input) returns @untainted int | error {
    if (input is int && input > 0) {
        return input;
    } else {
        log:printError("invalid data. expected a positive integer.");
        error e = error("invalid data");
        return e;
    }
}
