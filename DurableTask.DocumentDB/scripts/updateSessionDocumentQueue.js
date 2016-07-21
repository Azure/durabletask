// updates the orchestration and 

function (sessionDocumentId,
    orchestrationQueueDeletedMessages, 
    orchestrationQueueAddedMessages,
    activityQueueDeletedMessages,
    activityQueueAddedMessages) {
    
    var context = getContext();
    var response = context.getResponse();
    var collection = context.getCollection();

    var query = 'SELECT * FROM c WHERE c.id = "' + sessionDocumentId + '"'; 

    var accept = collection.queryDocuments(collection.getSelfLink(), query, {},
        function (err, documents, responseOptions) {
            if (err) throw new Error("Error" + err.message);

            if (documents.length === 0) {
                throw "sessionDocument " + sessionDocumentId;
            }   

            if (documents.length > 1) {
                throw "Potential data corruption; query for sessionDocument " + sessionDocumentId + " resulted in multiple documents";
            }   

            var sessionDocument = documents[0];

            sessionDocument.orchestrationQueue = item.orchestrationQueue;
            sessionDocument.activityQueue = item.activityQueue;

            sessionDocument.activityQueue = mergeQueue(sessionDocument.activityQueue,
                activityQueueDeletedMessages,
                activityQueueAddedMessages);

            sessionDocument.orchestrationQueue = mergeQueue(sessionDocument.orchestrationQueue,
                orchestrationQueueDeletedMessages,
                orchestrationQueueAddedMessages);

            var accept2 = collection.replaceDocument(sessionDocument._self, sessionDocument, 
                function (err, docReplaced) {
                    if (err) throw "Unable to update, aborting: " + err;
                    response.setBody(docReplaced);
                });
    
            if (!accept2) throw "Unable to update, aborting";
        });

    if (!accept) throw "Unable to update, aborting";

    // AFFANDAR : TODO : update time
    function mergeQueue(queue, toDelete, toAdd) {
        
        if (queue == null) {

            // don't change a thing if there was nothing to add and there was nothing in the queue to begin with
            if (!toAdd) {
                return null;
            }

            queue = []
        }

        var index = queue.length;

        // AFFANDAR : TODO : validate that element actually exists
        while (index--) {
            if (toDelete) {
                toDelete.forEach(function(value) {
                    if (queue[index].messageId === value.messageId) {
                        queue.splice(index, 1);
                    }
                });
            }
        }

        // AFFANDAR : TODO : any way to make this more performant?
        if (toAdd) {
            toAdd.forEach(function (value) {
                var alreadyPresent = false;
                queue.forEach(function(qvalue) {
                    if(qvalue.messageId === value.messageId) {
                        alreadyPresent = true;
                        throw "Message with id: " + value.messageId + " aleady exists in target queue";
                    }
                });
                queue.push(value);
            });
        }
        

        return queue;
    }
}