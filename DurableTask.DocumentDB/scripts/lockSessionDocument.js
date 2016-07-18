function (forOrchestration) {
            
    var context = getContext();
    var response = context.getResponse();
    var collection = context.getCollection();

    var query = 'SELECT * FROM c WHERE c.sessionLock = null AND c.orchestrationQueue != null AND c.documentType = "SessionDocument" ORDER BY c.orchestratorQueueLastUpdatedTimeUtc ASC'; 

    if(!forOrchestration)
    {
        query = 'SELECT * FROM c WHERE c.sessionLock = null AND c.activityQueue != null AND c.documentType = "SessionDocument" ORDER BY c.activityQueueLastUpdatedTimeUtc ASC'; 
    }

    var accept = collection.queryDocuments(collection.getSelfLink(), query, {},
        function (err, documents, responseOptions) {
            if (err) throw new Error("Error" + err.message);

            if (documents.length == 0) {
                getContext().getResponse().setBody(null);
                return;
            }   

            var item = documents[0];
            item.sessionLock = 
            { 
                LockToken : guid(),
                LockedUntilUtc : new Date()
            }
        
            var accept2 = collection.replaceDocument(item._self, item,
                function (err, docReplaced) {
                    if (err) throw "Unable to lock, aborting";
                    response.setBody(docReplaced);
                    //response.setValue("etag", docReplaced.etag);
                });
    
            if (!accept2) throw "Unable to lock, abort";
    
        });

    if (!accept) throw "Unable get orchestration workitem, abort";
            
    function guid() {
        return s4() + s4() + '-' + s4() + '-' + s4() + '-' +
          s4() + '-' + s4() + s4() + s4();
    }
        
    function s4() {
        return Math.floor((1 + Math.random()) * 0x10000)
          .toString(16)
          .substring(1);
    }
}