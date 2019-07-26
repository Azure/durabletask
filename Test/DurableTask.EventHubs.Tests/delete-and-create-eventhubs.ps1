# TODO this file is only for quick and dirty getting started, we actually want this stuff to happen programmatically

# Replace these to match your project

$NameSpace = 'sbeventhubs2'
$ResourceGroup = 'sb-durablefunctions'

# This deletes and then creates the eventhubs.

echo "Deleting existing EventHubs..."
az eventhubs eventhub delete --namespace-name $NameSpace --resource-group $ResourceGroup --name Partitions
az eventhubs eventhub delete --namespace-name $NameSpace --resource-group $ResourceGroup --name Clients0
az eventhubs eventhub delete --namespace-name $NameSpace --resource-group $ResourceGroup --name Clients1
az eventhubs eventhub delete --namespace-name $NameSpace --resource-group $ResourceGroup --name Clients2
az eventhubs eventhub delete --namespace-name $NameSpace --resource-group $ResourceGroup --name Clients3

echo "Creating fresh EventHubs..."
az eventhubs eventhub create --namespace-name $NameSpace --resource-group $ResourceGroup --name Partitions --message-retention 1 --partition-count 32
az eventhubs eventhub create --namespace-name $NameSpace --resource-group $ResourceGroup --name Clients0 --message-retention 1 --partition-count 32
az eventhubs eventhub create --namespace-name $NameSpace --resource-group $ResourceGroup --name Clients1 --message-retention 1 --partition-count 32
az eventhubs eventhub create --namespace-name $NameSpace --resource-group $ResourceGroup --name Clients2 --message-retention 1 --partition-count 32
az eventhubs eventhub create --namespace-name $NameSpace --resource-group $ResourceGroup --name Clients3 --message-retention 1 --partition-count 32

echo "Done."

# NOTE
#
# If clearing eventhubs as above, the functions' consumer positions, which are stored in blob storage, also need to be cleared. 
# The easy way to do this is to remove the entire blob folder
#
# (webjobs storage account)/azurewebjobs-eventhub/XXX.servicebus.windows.net
#
# where XXX is $NameSpace
