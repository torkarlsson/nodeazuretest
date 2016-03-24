var azure = require('azure-storage');
require('sugar');


var tableSvc = azure.createTableService('evrystoragetest001', 'upabFKzPK4nW7mxvJ318IrHirk2ND12SFjEGQHwkn1cgTpaASgYGxDNEyQmU39O+047Yrvn4VmIDTta5MCPTIg==');

var tableName = 'mytable' + Date.create().format('{ss}{ms}');


function createTable(){
    tableSvc.createTableIfNotExists(tableName, function (error, result, response) {
        if (error) {
            // Table exists or created
            console.log(error);
            return;;
        }
        
        console.log(Date.create().format('{mm}:{ss}:{ms}'));
        sendBatches();
        console.log(Date.create().format('{mm}:{ss}:{ms}'));

        getResult();
    });
}


var batches = new Array();

function createBatches() {
    var entGen = azure.TableUtilities.entityGenerator;
    
    for (var x = 1; x <= 1000; x++) {
        
        var batch = new azure.TableBatch();
        var largeString = 'extra space'.padRight(400, '*');
        
        for (var i = 0; i < 100; i++) {
            var task = {
                PartitionKey: entGen.String('hometasks3'),
                RowKey: entGen.String(x.toString() + '-' + i.toString()),
                description: entGen.String('take out the trash ' + i.toString()),
                dueDate: entGen.DateTime(new Date(Date.UTC(2015, 6, 20))),
                extraInfo: entGen.String(largeString),
            };

            batch.insertOrMergeEntity(task, { echoContent: false });
        };
        
        batches.push(batch);
    };

    console.log('Size: ' + memorySizeOf(batches));
}

function sendBatches() {
    var async = require('async');
    
    async.forEach(batches
    , function batchIterator(batch, callback) {
        tableSvc.executeBatch(tableName, batch, function (error, result, response) {
            if (!error) {
                callback(null);
                //console.log(Date.create().format('{mm}:{ss}:{ms}'));
            } else {
                callback(error);
                console.log(error);
            }
        });
    }, function (error) {
        if (!error) {
        }
    }
    );
}

createBatches();
createTable();



//tableSvc.executeBatch('mytable', batch, function (error, result, response) {
//    var res = result;
//    var resp = response;
//    var err = error;
//    if (!error) { }
//});

//tableSvc.insertEntity('mytable', task, function (error, result, response) {
//    if (!error) {
//        // Entity inserted
//    }
//});

function getResult() {
    var query = new azure.TableQuery()
  .top(5)
  .where('PartitionKey eq ?', 'hometasks3')
  .and('RowKey eq ?', '1-5');
    
    tableSvc.queryEntities(tableName, query, null, function (error, result, response) {
        if (!error) {
            var res = result.entries;
            console.log(res[0]);
        }
    });
}


function memorySizeOf(obj) {
    var bytes = 0;
    
    function sizeOf(obj) {
        if (obj !== null && obj !== undefined) {
            switch (typeof obj) {
                case 'number':
                    bytes += 8;
                    break;
                case 'string':
                    bytes += obj.length * 2;
                    break;
                case 'boolean':
                    bytes += 4;
                    break;
                case 'object':
                    var objClass = Object.prototype.toString.call(obj).slice(8, -1);
                    if (objClass === 'Object' || objClass === 'Array') {
                        for (var key in obj) {
                            if (!obj.hasOwnProperty(key)) continue;
                            sizeOf(obj[key]);
                        }
                    } else bytes += obj.toString().length * 2;
                    break;
            }
        }
        return bytes;
    };
    
    function formatByteSize(bytes) {
        if (bytes < 1024) return bytes + " bytes";
        else if (bytes < 1048576) return (bytes / 1024).toFixed(3) + " KiB";
        else if (bytes < 1073741824) return (bytes / 1048576).toFixed(3) + " MiB";
        else return (bytes / 1073741824).toFixed(3) + " GiB";
    };
    
    return formatByteSize(sizeOf(obj));
};