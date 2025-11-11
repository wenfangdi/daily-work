async def add_fiix_object(session_obj, body_content):
    # Build Fiix URL using global variables
    fiix_url = f"{subdomain}/api/?action=AddResponse&appKey={appKey}&accessKey={accessKey}&signatureMethod=HmacSHA256&signatureVersion=1"

    auth_message = fiix_url.partition("https://")[-1].encode('utf-8')
    auth_return = hmac.new(secretKey.encode('utf-8'), auth_message, hashlib.sha256).hexdigest()
    auth_headers = {"Content-Type": "text/plain", "Authorization": auth_return}
    
    resp = await session_obj.post(
        url=fiix_url,
        json=body_content,
        headers=auth_headers,
        timeout=60
    )
    print('Posted addresponse' ,body_content)
    
    return resp
  async def create_new_wo(session_obj, object_json) -> int:

    answer = await add_fiix_object(session_obj,{
            '_maCn': 'AddRequest', 
            'clientVersion': {'major': 2, 'minor': 3, 'patch': 1}, 
            'className': 'WorkOrder', 
            'fields' : 'id,strCode',
            "object": object_json
          } )
        wo_objects = await answer.json(content_type = None)
        return wo_objects['object']['id']

async def link_wo_to_asset(session_obj, wo_id, asset_id):

    answer = await add_fiix_object(session_obj,{
            '_maCn': 'AddRequest', 
            'clientVersion': {'major': 2, 'minor': 3, 'patch': 1}, 
            'className': 'WorkOrderAsset', 
            'fields' : 'id,intWorkOrderID',
            "object": {
                "className": "WorkOrderAsset",
                "intAssetID": str(asset_id),
                "intWorkOrderID": wo_id
            }
          } )
        return await answer.json(content_type = None)

for row in test_growth.itertuples(index=False):
    description = row.station_name + ' During Growth ' + str(row.id_run_growth) + ' around the ' + str(row.RD) + ' Hour Detected Max Q Diff ' 
    description += str(round(row.Max_Q_diff,2)) + '\n'
    description += "The previous Growth's Max Q Diff was "  + str(round(row.prev_Max_Q_diff,2)) + '\n'
    description += 'Therefore Requesting a TNU Check'
    object_json = {
        'className': 'WorkOrder',
        'intMaintenanceTypeID' : 403297, #Inspection
        'intPriorityID' :  205937, #Medium
        'intRequestedByUserID' : 1228327, #Fangdi
        'intSiteID' : row.intSiteID, #WA
        "intWorkOrderStatusID": 326500,  #:assigned
        'strDescription' : description
    }
    async with await get_clientsession() as session_obj:
        wo_id = await create_new_wo(session_obj, object_json)
        await link_wo_to_asset(session_obj, wo_id,row.fiix_asset_id)
    print(wo_id, 'wo created for growth' , row.id_run_growth)
