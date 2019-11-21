const AWS = require("aws-sdk");

if (process.env.NODE_ENV === 'devel') {
    const config = require('../config.js');
    AWS.config.update({
        accessKeyId: config.aws.accessKeyId,
        secretAccessKey: config.aws.secretAccessKey,
        region: "ap-northeast-2",
    });
} else {
    AWS.config.update({
        region: "ap-northeast-2",
    });
}

exports.handler = async (gevent, context) => {
    const docClient = new AWS.DynamoDB.DocumentClient();
    const event = gevent.queryStringParameters || {};
    const count = event.count || 1000;
    if (count < 5) {
        return {
            statusCode: 400,
            body: JSON.stringify({
                "status": "error",
                "error": "요청 인자 count가 너무 작습니다."
            })
        };
    }
    if (count > 1000) {
        return {
            statusCode: 400,
            body: JSON.stringify({
                "status": "error",
                "error": "요청 인자 count가 너무 큽니다."
            })
        };
    }
    const _southWest = event.sw;
    const _northEast = event.ne;
    let bounds = null;
    if (_southWest && _northEast) {
        const sw = _southWest.split(",");
        const ne = _northEast.split(",");
        bounds = [...ne, ...sw];
    }
    const params = {
        TableName: "CrowdSensorCloudDevice",
        ScanIndexForward: false,
        Limit: count
    };
    if (bounds) {
        params["IndexName"] = "lat-long-index";
        params["FilterExpression"] = "#lat <= :north and #lng <= :east and #lat >= :south and #lng >= :west";
        params["ExpressionAttributeNames"] = {
            "#lat": "lat",
            "#lng": "long",
        };
        params["ExpressionAttributeValues"] = {
            ":north": parseFloat(bounds[0]),
            ":east": parseFloat(bounds[1]),
            ":south": parseFloat(bounds[2]),
            ":west": parseFloat(bounds[3]),
        };
    }

    try {
        return await new Promise((resolve, reject) => {
            docClient.scan(params, function (err, data) {
                if (err) {
                    reject("Unable to query. Error: " + JSON.stringify(err, null, 2));
                } else if (event.device && data.Items.length == 0) {
                    reject(`${event.device}의 자료가 없습니다.`);
                } else if (data.Items.length == 0) {
                    reject(`검색된 자료가 없습니다.`);
                } else {
                    let cnt = 0;
                    let results = [];
                    let rows = [];
                    data.Items.sort(function (a, b) {
                        return -1 * (a.timestamp < b.timestamp ? -1 : a.timestamp === b.timestamp ? 0 : 1);
                    }).forEach(function (data) {
                        if (cnt < count) {
                            delete data.pin;
                            results.push(data);
                        }
                        cnt++;
                    });
                    if (process.env.NODE_ENV === 'devel') {
                        console.log(results);
                    }
                    resolve({
                        statusCode: 200,
                        headers: {
                            "Cache-Control": "max-age=3600",
                        },
                        body: JSON.stringify({
                            "status": "success",
                            "count": cnt,
                            "results": results
                        })
                    });
                }
            });
        });
    } catch (err) {
        if (process.env.NODE_ENV === 'devel') {
            console.error(err);
        }
        return {
            statusCode: 400,
            headers: {
                "Cache-Control": "max-age=300",
            },
            body: JSON.stringify({
                "status": "error",
                "error": (typeof err === 'string') ? err : JSON.stringify(err)
            })
        };
    }
};


if (process.env.NODE_ENV === 'devel') {
    exports.handler({
        "body": "eyJ0ZXN0IjoiYm9keSJ9",
        "resource": "/{proxy+}",
        "path": "/path/to/resource",
        "httpMethod": "POST",
        "isBase64Encoded": true,
        "queryStringParameters": {
            "ne": "37.613245557430965,127.07048892974855",
            "sw": "37.592981581617025,126.98809146881105"
        },
        "pathParameters": {
            "proxy": "/path/to/resource"
        },
        "stageVariables": {
            "baz": "qux"
        },
        "headers": {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate, sdch",
            "Accept-Language": "en-US,en;q=0.8",
            "Cache-Control": "max-age=0",
            "CloudFront-Forwarded-Proto": "https",
            "CloudFront-Is-Desktop-Viewer": "true",
            "CloudFront-Is-Mobile-Viewer": "false",
            "CloudFront-Is-SmartTV-Viewer": "false",
            "CloudFront-Is-Tablet-Viewer": "false",
            "CloudFront-Viewer-Country": "US",
            "Host": "1234567890.execute-api.ap-northeast-2.amazonaws.com",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Custom User Agent String",
            "Via": "1.1 08f323deadbeefa7af34d5feb414ce27.cloudfront.net (CloudFront)",
            "X-Amz-Cf-Id": "cDehVQoZnx43VYQb9j2-nvCh-9z396Uhbp027Y2JvkCPNLmGJHqlaA==",
            "X-Forwarded-For": "127.0.0.1, 127.0.0.2",
            "X-Forwarded-Port": "443",
            "X-Forwarded-Proto": "https"
        },
        "requestContext": {
            "accountId": "123456789012",
            "resourceId": "123456",
            "stage": "prod",
            "requestId": "c6af9ac6-7b61-11e6-9a41-93e8deadbeef",
            "requestTime": "09/Apr/2015:12:34:56 +0000",
            "requestTimeEpoch": 1428582896000,
            "identity": {
                "cognitoIdentityPoolId": null,
                "accountId": null,
                "cognitoIdentityId": null,
                "caller": null,
                "accessKey": null,
                "sourceIp": "127.0.0.1",
                "cognitoAuthenticationType": null,
                "cognitoAuthenticationProvider": null,
                "userArn": null,
                "userAgent": "Custom User Agent String",
                "user": null
            },
            "path": "/prod/path/to/resource",
            "resourcePath": "/{proxy+}",
            "httpMethod": "POST",
            "apiId": "1234567890",
            "protocol": "HTTP/1.1"
        }
    });
}
