// removes the session lock if it exists
// if session lock expired or does not exist then throw an exception
// if session lock checks out then updates the session document as per the supplied doc

// AFFANDAR : TODO : better error messages and think through error handling strategy
function (sessionDocument, unlock) {
            
    var context = getContext();
    var response = context.getResponse();
    var collection = context.getCollection();
    var expired = false;

    var query = 'SELECT * FROM c WHERE c.id = "' + sessionDocument.id + '" AND c.sessionLock != null AND c.sessionLock.LockToken = "' + sessionDocument.sessionLock.LockToken + '"'; 

    var accept = collection.queryDocuments(collection.getSelfLink(), query, {},
        function (err, documents, responseOptions) {
            if (err) throw new Error("Error" + err.message);

            if (documents.length === 0) {
                throw "sessionDocument " + sessionDocument.id + " not found or session lock lost";
            }   

            if (documents.length > 1) {
                throw "Potential data corruption; query for sessionDocument " + sessionDocument.id + " resulted in multiple documents";
            }   

            var item = documents[0];

            if (unlock) {

                // only care about session locks if we were unlocking the doc as well
                if(item.sessionLock.LockedUntilUtc < new Date()) {
                    expired = true;
                }

                sessionDocument.sessionLock = null;
            }

            var accept2 = collection.replaceDocument(item._self, sessionDocument, 
                {
                    etag : sessionDocument._etag
                },
                function (err, docReplaced) {
                    if (err) throw "Unable to update, aborting: " + err;
                    response.setBody(docReplaced);
                });
    
            if (!accept2) throw "Unable to update, aborting";
        });

    if (!accept) throw "Unable to update, aborting";

    if(expired) {
        throw "Session lock lost";
    }
}