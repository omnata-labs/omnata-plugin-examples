"""
Module containing the Zoho CRM plugin
"""
from typing import Dict, List, Tuple
from logging import getLogger
import pandas
import requests
import io
import zipfile
import time
import csv
from omnata_plugin_runtime.forms import (
    ConnectionMethod,
    FormInputField,
    FormDropdownField,
    FormFieldMappingSelector,
    FormOption,
    InboundSyncConfigurationForm,
    OutboundSyncConfigurationForm,
    SyncConfigurationParameters,
    SecurityIntegrationTemplateAuthorizationCode,
    DynamicFormOptionsDataSource,
    StaticFormOptionsDataSource,
)
from omnata_plugin_runtime.omnata_plugin import (
    OmnataPlugin,
    PluginManifest,
    ConnectResponse,
    OutboundSyncRequest,
    InboundSyncRequest,
    StreamConfiguration,
    managed_outbound_processing,
    StoredStreamConfiguration,
    managed_inbound_processing
)
from omnata_plugin_runtime.configuration import (
    ConnectionConfigurationParameters,
    CreateSyncStrategy,
    UpdateSyncStrategy,
    UpsertSyncStrategy,
    CreateSyncAction,
    UpdateSyncAction,
    OutboundSyncConfigurationParameters,
    InboundSyncConfigurationParameters,
    InboundSyncStrategy,
    OutboundSyncStrategy
)
from omnata_plugin_runtime.rate_limiting import ApiLimits, too_many_requests_hook, RequestRateLimit, HttpRequestMatcher

logger = getLogger(__name__)


class ZohoCrmPlugin(OmnataPlugin):
    """
    An example plugin for the Zoho CRM product.
    Uses the bulk read and write API to load records into Zoho modules, and extract them.
    """

    def get_manifest(self) -> PluginManifest:
        """
        Returns the Plugin Manifest, provides information about the plugin.
        """
        return PluginManifest(
            plugin_id="zoho_crm",
            plugin_name="Zoho CRM",
            developer_id="omnata",
            developer_name="Omnata",
            docs_url="https://docs.omnata.com",
            supports_inbound=True,
            supported_outbound_strategies=[CreateSyncStrategy(), UpdateSyncStrategy(), UpsertSyncStrategy()],
        )

    def connection_form(self) -> List[ConnectionMethod]:
        """
        Returns information to populate the connection form.
        Zoho supports OAuth, so we return an OAuth template which will guide the user through creation of a security integration.
        """
        return [
            ConnectionMethod(
                name="OAuth",
                fields=[
                    FormDropdownField(name='data_center_domain',label='Data Center',
                    required=True,
                    data_source=StaticFormOptionsDataSource(values=[
                        FormOption(value='www.zohoapis.com',label='USA',default=True),
                        FormOption(value='www.zohoapis.com.au',label='Australia'),
                        FormOption(value='www.zohoapis.eu',label='Europe'),
                        FormOption(value='www.zohoapis.in',label='India'),
                        FormOption(value='www.zohoapis.com.cn',label='China'),
                        FormOption(value='www.zohoapis.jp',label='Japan')
                    ])),
                    FormDropdownField(name='product_edition',label='Product Edition',
                    help_text="Used to calculate rate limit quota",
                    required=True,
                    data_source=StaticFormOptionsDataSource(values=[
                        FormOption(value='free',label='Free Edition',default=True),
                        FormOption(value='standard_starter',label='Standard/Starter Edition'),
                        FormOption(value='professional',label='Professional'),
                        FormOption(value='enterprise',label='Enterprise/Zoho One'),
                        FormOption(value='ultimate',label='Ultimate/CRM Plus'),
                    ])),
                FormInputField(name='user_count',label='Purchased Users Count',default_value="100",help_text="Used to calculate rate limit quota",required=True),
                FormInputField(name='addon_credits',label='Addon API credits purchased',default_value="0",help_text="Used to calculate rate limit quota",required=True)
                ],
                oauth_template=SecurityIntegrationTemplateAuthorizationCode(
                    oauth_client_id="<Client ID>",
                    oauth_client_secret="<Client Secret>",
                    oauth_token_endpoint="https://<Account domain (accounts.zoho.x)>/oauth/v2/token",
                    oauth_authorization_endpoint="https://<Account domain (accounts.zoho.x)>/oauth/v2/auth",
                    oauth_allowed_scopes=["ZohoCRM.bulk.ALL",
                                          "ZohoCRM.settings.modules.READ",
                                          "ZohoCRM.settings.fields.READ",
                                          "ZohoCRM.org.READ",
                                          "ZohoFiles.files.ALL",
                                          "ZohoCRM.modules.ALL"],
                ),
            )
        ]

    def network_addresses(self, parameters: ConnectionConfigurationParameters) -> List[str]:
        """
        Returns the network addresses that the plugin will connect to.
        The user will be instructed to create a network rule to allow the plugin to connect to these addresses.
        """
        data_center_domain=parameters.get_connection_parameter('data_center_domain').value
        return [data_center_domain,
                data_center_domain.replace('www','content'),
                data_center_domain.replace('www','download').replace('.zohoapis.','.zoho.')]

    def connect(self, parameters: ConnectionConfigurationParameters) -> ConnectResponse:
        """
        Tests the connection to Zoho CRM, using the provided parameters
        """
        (base_url,headers) = self.get_auth_details(parameters)
        response = requests.get(f"{base_url}/crm/v5/org",headers=headers)
        if response.status_code != 200:
            raise ValueError(f"Error connecting to Zoho CRM: {response.text}")
        return ConnectResponse(connection_parameters={
            "org_id": response.json()['org'][0]['zgid'],
        })

    def get_auth_details(self, parameters: ConnectionConfigurationParameters) -> Tuple[str,Dict]:
        """
        Returns a tuple containing the API base url and the header dict to include in requests
        """
        data_center_domain=parameters.get_connection_parameter('data_center_domain').value
        access_token=parameters.get_connection_secret('access_token').value
        return (f"https://{data_center_domain}",{
                "Authorization": f"Zoho-oauthtoken {access_token}"
            })

    def api_limits(self, parameters: SyncConfigurationParameters) -> List[ApiLimits]:
        """
        Zoho CRM has a complex rate limiting system, which is based on the product edition, the number of users
        and the number of API credits purchased.
        """
        product_edition=parameters.get_connection_parameter('product_edition').value
        user_count=int(parameters.get_connection_parameter('user_count').value)
        addon_credits=int(parameters.get_connection_parameter('addon_credits').value)
        api_credits = self.api_credits(product_edition,user_count,addon_credits)
        concurrency_limit = self.concurrency_limit(product_edition)
        return [
            ApiLimits(
                endpoint_category="Bulk Write Initialize",
                request_matchers=[HttpRequestMatcher(http_methods=["POST"],url_regex="/crm/bulk/v\d/write")],
                request_rates=[RequestRateLimit(request_count=api_credits / 500, time_unit="day", unit_count=1)],
            ),
            ApiLimits(
                endpoint_category="Bulk Read Initialize",
                request_matchers=[HttpRequestMatcher(http_methods=["POST"],url_regex="/crm/bulk/v\d/read")],
                request_rates=[RequestRateLimit(request_count=api_credits / 50, time_unit="day", unit_count=1)],
            )
            ,
            ApiLimits(
                endpoint_category="Record Insert/Update/Read",
                request_matchers=[HttpRequestMatcher(http_methods=["GET","POST","PUT"],url_regex="/crm/v\d/\w+$")],
                # this is approximate, the limit is actually 1 credit per 10 records, with 100 records max per request
                request_rates=[RequestRateLimit(request_count=api_credits / 10, time_unit="day", unit_count=1)],
            )
        ]
    
    def api_credits(self,product_edition:str,user_count:int,addon_credits:int) -> int:
        """
        API Credit calculation, from the following table:
        https://www.zoho.com/crm/developer/docs/api/v5/api-limits.html
        """
        if product_edition=='free':
            return 5000
        elif product_edition=='standard_starter':
            return 50000 + (user_count*250) + addon_credits
        elif product_edition=='professional':
            return 50000 + (user_count*500) + addon_credits
        elif product_edition=='enterprise':
            return 50000 + (user_count*1000) + addon_credits
        elif product_edition=='ultimate':
            return 50000 + (user_count*2000) + addon_credits
        else:
            raise ValueError(f"Unknown product edition {product_edition}")
    
    def concurrency_limit(self,product_edition:str) -> int:
        """
        Concurrency limit calculation, from the following table:
        https://www.zoho.com/crm/developer/docs/api/v5/api-limits.html
        """
        if product_edition=='free':
            return 5
        elif product_edition=='standard_starter':
            return 10
        elif product_edition=='professional':
            return 15
        elif product_edition=='enterprise':
            return 20
        elif product_edition=='ultimate':
            return 25
        else:
            raise ValueError(f"Unknown product edition {product_edition}")
        
    def outbound_configuration_form(self, parameters: OutboundSyncConfigurationParameters) -> OutboundSyncConfigurationForm:
        """
        Returns a form which will be presented to the user when configuring the outbound sync.
        """
        fields_list = [
            FormDropdownField(name='module',label='Module',
                data_source=DynamicFormOptionsDataSource(
                    source_function=self.fetch_module_form_options))
        ]
        update_or_upsert = parameters.sync_strategy in [UpdateSyncStrategy(),UpsertSyncStrategy()]
        if update_or_upsert:
            fields_list.append(FormDropdownField(name='find_by',label='Unique Field',help_text="Matches existing records when updating or upserting",
                depends_on='module',
                data_source=DynamicFormOptionsDataSource(
                    source_function=self.fetch_module_fields_form_options)))
        
        return OutboundSyncConfigurationForm(
            fields=fields_list,
            mapper=FormFieldMappingSelector(depends_on='find_by' if update_or_upsert else 'module',
                data_source=DynamicFormOptionsDataSource(
                    source_function=self.fetch_module_fields_form_options)))
    
    def fetch_module_form_options(self, parameters: OutboundSyncConfigurationParameters) -> List[FormOption]:
        """
        Populates the 'Module' dropdown field during sync configuration.
        """
        modules = self.fetch_modules_from_zoho(parameters)
        return sorted([
            FormOption(value=module['module_name'],
                label=module['singular_label'],
                metadata=module) for module in modules
        ], key=lambda d: d['label'])
    
    def fetch_module_fields_form_options(self, parameters: OutboundSyncConfigurationParameters) -> List[FormOption]:
        """
        Populates the list of fields to map to during outbound sync configuration.
        """
        module_name=parameters.get_sync_parameter('module').value
        fields = self.fetch_fields_for_module(parameters,module_name)      
        return sorted([
            FormOption(
                value=field['api_name'],
                label=field['display_label'],
                # If there is a find_by parameter selected, the field is considered mandatory for the mapper
                required=field['system_mandatory'] or parameters.sync_parameter_exists('find_by') and parameters.get_sync_parameter('find_by').value==field['api_name'],
                metadata=field) for field in fields
        ], key=lambda d: d['label'])
    

    def fetch_modules_from_zoho(self, parameters: OutboundSyncConfigurationParameters) -> List[Dict]:
        """
        Fetches a list of modules from Zoho CRM.
        """
        (base_url, headers) = self.get_auth_details(parameters)
        modules_response = requests.get(f"{base_url}/crm/v3/settings/modules",headers=headers)
        if modules_response.status_code != 200:
            raise ValueError(f"Error fetching Zoho CRM module list: {modules_response.status_code}: {modules_response.text}")
        return modules_response.json()['modules']

    def fetch_fields_for_module(self, parameters: OutboundSyncConfigurationParameters, module_name:str) -> List[Dict]:
        """
        Fetches a list of fields from Zoho CRM.
        """
        (base_url, headers) = self.get_auth_details(parameters)
        fields_response = requests.get(f"{base_url}/crm/v3/settings/fields?module={module_name}&type=all",headers=headers)        
        if fields_response.status_code != 200:
            raise ValueError(f"Error fetching Zoho CRM fields for module {module_name}: {fields_response.status_code}: {fields_response.text}")
        return fields_response.json()['fields']
    
    
    def sync_outbound(self, parameters: OutboundSyncConfigurationParameters, outbound_sync_request: OutboundSyncRequest):
        """
        This method is called by the Omnata engine whenever an outbound sync is performed.
        """
        (base_url, headers) = self.get_auth_details(parameters)
        # bulk APIs require the org id, this was stored during initial connection
        org_id = parameters.get_connection_parameter('org_id').value
        # determine which module we're writing data for
        module_name = parameters.get_sync_parameter('module').value
        # find_by will have been provided if the sync strategy is update/upsert
        find_by:str = None
        if parameters.sync_parameter_exists('find_by'):
            find_by = parameters.get_sync_parameter('find_by').value
        
        # our plugin only supports insert, update and upsert so we should only ever be given Create and Update actions
        records = outbound_sync_request.get_records(batched=True,sync_actions=[CreateSyncAction(),UpdateSyncAction()])
        self.record_upload(data_frame=records,base_url=base_url,headers=headers,org_id=org_id,
                           module_name=module_name,sync_strategy=parameters.sync_strategy,find_by=find_by)

    @managed_outbound_processing(concurrency=5, batch_size=25000)
    def record_upload(self, data_frame: pandas.DataFrame, base_url:str,headers:dict,org_id:str,module_name:str,
                            sync_strategy:OutboundSyncStrategy,find_by:str) -> pandas.DataFrame:
        """
        Uploads records in batches of 25,000 to Zoho CRM, using the bulk write API.
        See https://www.zoho.com/crm/developer/docs/api/v5/bulk-write/overview.html
        """
        # expand the JSON representation out to dataframe columns
        logger.info(f"Uploading {len(data_frame)} records to Zoho CRM")
        records_expanded = pandas.json_normalize(data_frame['TRANSFORMED_RECORD'])
        # convert the dataframe to CSV and zip it in memory
        csv_contents = records_expanded.to_csv(index=False)
        in_memory_zip = io.BytesIO()
        with zipfile.ZipFile(in_memory_zip, mode="w",compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr('file.csv',csv_contents)
        in_memory_zip.seek(0) # rewind to start of buffer
        logger.info(f"Zip file size: {len(in_memory_zip.getvalue())}")
        headers['feature'] = 'bulk-write'
        headers['X-CRM-ORG'] = org_id
        files = {'file.csv.zip':in_memory_zip}
        content_base_url = base_url.replace('www','content') # upload API goes to a slighty different host
        upload_response = requests.post(f"{content_base_url}/crm/v3/upload",headers=headers,files=files,hooks={'response':too_many_requests_hook()})
        if upload_response.status_code!=200:
            raise ValueError(f"Error {upload_response.status_code} uploading CSV: {upload_response.text}")
        upload_response_json = upload_response.json()
        if upload_response_json['status']!='success':
            raise ValueError(f"Status of {upload_response_json['status']} in response to CSV upload. Code: {upload_response_json['code']}, message: {upload_response_json['message']}")
        file_id = upload_response_json['details']['file_id']
        logger.info(f"Uploaded file id {file_id} to content API")
        # create a bulk import request, which advises Zoho how to import the uploaded file
        bulk_import_request = {
            "operation": sync_strategy.name.lower(),
            "ignore_empty": True,
            "resource": [
                {
                "type": "data",
                "module": {
                    "api_name": module_name
                },
                "file_id": file_id,
                "field_mappings": [{"api_name":x[1],"index":x[0]} for x in enumerate(records_expanded.columns)]
                }
            ]
        }
        if find_by is not None:
            bulk_import_request['resource'][0]['find_by'] = find_by

        import_response = requests.post(f"{base_url}/crm/bulk/v3/write",headers=headers,json=bulk_import_request,hooks={'response':too_many_requests_hook()})
        if import_response.status_code!=201:
            raise ValueError(f"Error {import_response.status_code} uploading CSV: {import_response.text}")
        import_response_json = import_response.json()
        # import id is what we use to track the import
        import_id = import_response_json['details']['id']
        logger.info(f"Created import job with id {import_id}")
        retries_remaining = 60
        retry_wait = 5
        download_url = None
        # check the status of the import job until it completes, or we give up waiting
        while retries_remaining > 0:
            import_status_response = requests.get(f"{base_url}/crm/bulk/v3/write/{import_id}",headers=headers,hooks={'response':too_many_requests_hook()})
            import_status_json = import_status_response.json()
            import_status = import_status_json['status']
            print(f"import status: {import_status}")
            if import_status=='COMPLETED':
                download_url = import_status_json['result']['download_url']
                break
            time.sleep(retry_wait)
            retries_remaining = retries_remaining - 1
        if retries_remaining == 0:
            raise ValueError('Import timed out')
        # download the import results
        import_results = requests.get(download_url,headers=headers,hooks={'response':too_many_requests_hook()})
        # unzip the results and load them into a dataframe
        with zipfile.ZipFile(io.BytesIO(import_results.content)) as zip_archive:
            with zip_archive.open(zip_archive.infolist()[0]) as file_in_zip:
                results_df = pandas.read_csv(file_in_zip)
                logger.info(f"Retrieved {len(results_df)} results from import job")
                # the CSV that comes back from Zoho has the same records in the same order,
                # so we can concatenate the dataframes sideways to match them
                records_to_apply = pandas.concat([data_frame,results_df], axis=1)
        # consider each record a success if the status is ADDED or UPDATED
        records_to_apply['SUCCESS'] = False
        records_to_apply['SUCCESS'] = records_to_apply.apply(lambda x: True if x['STATUS'] in ('ADDED','UPDATED') else False, axis=1)
        # pass the Zoho record ID back as the app identifier
        records_to_apply['APP_IDENTIFIER']=records_to_apply['RECORD_ID'].astype(str)
        # pass the Zoho status back as a record result
        records_to_apply['RESULT'] = records_to_apply.apply(lambda x:{"status":x['STATUS'],"errors":x['ERRORS']},axis=1)
        return records_to_apply[['IDENTIFIER','APP_IDENTIFIER','SUCCESS','RESULT']]
    
    def inbound_configuration_form(self, parameters: InboundSyncConfigurationParameters) -> InboundSyncConfigurationForm:
        """
        Returns a form which will be presented to the user when configuring the inbound sync.
        There are no fields to configure for inbound syncs in this plugin.
        """
        return InboundSyncConfigurationForm(fields=[])

    def inbound_stream_list(self, parameters: InboundSyncConfigurationParameters) -> List[StreamConfiguration]:
        """
        Lists the streams available for inbound syncs, we provide the list of Zoho modules
        """
        modules:List[Dict] = self.fetch_modules_from_zoho(parameters)
        return [
            StreamConfiguration(
                stream_name=m['module_name'],
                supported_sync_strategies=[InboundSyncStrategy.FULL_REFRESH],
                source_defined_cursor=True,
                default_cursor_field="Modified_Time",
                source_defined_primary_key="Id",
                json_schema=self.json_schema_for_module(parameters,m['module_name']) \
                    if parameters.currently_selected_streams is not None and m['module_name'] in parameters.currently_selected_streams else None,
            ) for m in modules
        ]
    
    def json_schema_for_module(self, parameters: InboundSyncConfigurationParameters, module_name:str) -> Dict:
        """
        Builds a standard JSON-schema object from the fields in a Zoho module.
        """
        fields = self.fetch_fields_for_module(parameters,module_name)
        return {
            "$schema": "http://json-schema.org/draft-06/schema#",
            "type": "object",
            "additionalProperties": True,
            "properties": {
                field['field_label']:{"type": field['json_type']} for field in fields
            }
        }

    def sync_inbound(self, parameters: InboundSyncConfigurationParameters, inbound_sync_request: InboundSyncRequest):
        """
        This method is called by the Omnata engine whenever an inbound sync is performed.
        We defer the actual work to the fetch_records method, which is decorated with managed_inbound_processing.
        """
        self.fetch_records(inbound_sync_request.streams,
                            parameters=parameters,
                            inbound_sync_request=inbound_sync_request)

    @managed_inbound_processing(concurrency=5)
    def fetch_records(self,
            stream:StoredStreamConfiguration,
            parameters: InboundSyncConfigurationParameters,
            inbound_sync_request: InboundSyncRequest):
        """
        Fetches records for an individual stream.
        """
        (base_url, headers) = self.get_auth_details(parameters)
        bulk_import_request = {
            "query": {
                "module": {
                    "api_name": stream.stream_name
                }
            }
        }
        export_response = requests.post(f"{base_url}/crm/bulk/v5/read",headers=headers,json=bulk_import_request,hooks={'response':too_many_requests_hook()})
        if export_response.status_code!=201:
            raise ValueError(f"Error {export_response.status_code} creating bulk read job: {export_response.text}")
        export_response_json = export_response.json()
        # export id is what we use to track the export
        export_id = export_response_json['data'][0]['details']['id']
        retries_remaining = 60
        retry_wait = 5
        download_url = None
        # check the status of the export job until it completes, or we give up waiting
        while retries_remaining > 0:
            export_status_response = requests.get(f"{base_url}/crm/bulk/v5/read/{export_id}",headers=headers,hooks={'response':too_many_requests_hook()})
            export_status_json = export_status_response.json()
            export_status = export_status_json['data'][0]['state']
            logger.info(f"export status: {export_status}")
            
            if export_status=='COMPLETED':
                download_url = export_status_json['data'][0]['result']['download_url']
                break
            time.sleep(retry_wait)
            retries_remaining = retries_remaining - 1
        if retries_remaining == 0:
            raise ValueError(f"Export timed out after {retries_remaining*retry_wait} seconds")
        # download the export results
        export_results = requests.get(f"{base_url}{download_url}",headers=headers,hooks={'response':too_many_requests_hook()})
        if export_results.status_code!=200:
            raise ValueError(f"Error {export_results.status_code} reading results: {export_results.text}")
        # unzip the results and load them into a dataframe
        with zipfile.ZipFile(io.BytesIO(export_results.content)) as zip_archive:
            with zip_archive.open(zip_archive.infolist()[0]) as file_in_zip:
                # read the bytes from the file inside the zip file, decode them as UTF-8, and then parse them as CSV
                reader = csv.DictReader(io.StringIO(file_in_zip.read().decode('utf-8')))
                # convert to a list of dicts by iterating over the DictReader
                list_of_dicts = list(reader)
                # get the dict with the maximum "Modified_Time" value
                max_modified_time = max(list_of_dicts,key=lambda x:x['Modified_Time'])
                inbound_sync_request.enqueue_results(stream.stream.stream_name, list_of_dicts, {'max_modified_time': max_modified_time})

