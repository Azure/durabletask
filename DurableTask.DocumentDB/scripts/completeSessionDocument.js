// removes the session lock if it exists
// if session lock expired or does not exist then throw an exception
// if session lock checks out then updates the session document as per the supplied doc

// AFFANDAR : TODO : better error messages and think through error handling strategy
function(sessionDocument,
    orchestrationQueueDeletedMessages,
    orchestrationQueueAddedMessages,
    activityQueueDeletedMessages,
    activityQueueAddedMessages) {

    var context = getContext();
    var response = context.getResponse();
    var collection = context.getCollection();
    var expired = false;

    if (!sessionDocument.sessionLock || !sessionDocument.sessionLock.LockToken) {
        throw "No session lock specified in session document";
    }

    var query = 'SELECT * FROM c WHERE c.id = "' +
        sessionDocument.id +
        '" AND c.sessionLock != null AND c.sessionLock.LockToken = "' +
        sessionDocument.sessionLock.LockToken +
        '"';

    var accept = collection.queryDocuments(collection.getSelfLink(),
        query,
        {},
        function(err, documents, responseOptions) {
            if (err) throw new Error("Error" + err.message);

            if (documents.length === 0) {
                throw "sessionDocument " + sessionDocument.id + " not found or session lock lost";
            }

            if (documents.length > 1) {
                throw "Potential data corruption; query for sessionDocument " +
                    sessionDocument.id +
                    " resulted in multiple documents";
            }

            var item = documents[0];

            if (item.sessionLock.LockedUntilUtc < new Date()) {
                expired = true;
                item.sessionLock = null;
            } else {
                sessionDocument.sessionLock = null;

                sessionDocument.orchestrationQueue = item.orchestrationQueue;
                sessionDocument.activityQueue = item.activityQueue;

                sessionDocument.activityQueue = mergeQueue(sessionDocument.activityQueue,
                    activityQueueDeletedMessages,
                    activityQueueAddedMessages);

                sessionDocument.orchestrationQueue = mergeQueue(sessionDocument.orchestrationQueue,
                    orchestrationQueueDeletedMessages,
                    orchestrationQueueAddedMessages);
            }

            // AFFANDAR : TODO : no etag check required because the ownership of the lock that we already verified
            //              in the query already gives us concurrency check, in the meantime etag might have changed
            //               because of additional enqueues/dequeues in the orchestrator queues, however for those
            //              we are specifically doing a merge operation to resolve any conflicts
            //              transactional nature of sprocs makes that consistent
            var accept2 = collection.replaceDocument(item._self,
                expired ? item : sessionDocument,
                function(err, docReplaced) {
                    if (err) throw "Unable to update, aborting: " + err;
                    response.setBody(docReplaced);
                });

            if (!accept2) throw "Unable to update, aborting";
        });

    if (!accept) throw "Unable to update, aborting";

    if (expired) {
        throw "Session lock lost";
    }

    function mergeQueue(queue, toDelete, toAdd) {

        if (queue == null) {

            // don't change a thing if there was nothing to add and there was nothing in the queue to begin with
            if (!toAdd) {
                return null;
            }

            queue = [];
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
            toAdd.forEach(function(value) {
                var alreadyPresent = false;
                queue.forEach(function(qvalue) {
                    if (qvalue.messageId === value.messageId) {
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