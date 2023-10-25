"""
Module containing the Slack plugin
"""
from typing import List
from logging import getLogger
import pandas
from omnata_plugin_runtime.forms import (
    ConnectionMethod,
    FormDropdownField,
    FormJinjaTemplate,
    FormOption,
    InboundSyncConfigurationForm,
    OutboundSyncConfigurationForm,
    SyncConfigurationParameters,
    SecurityIntegrationTemplate,
    DynamicFormOptionsDataSource,
)
from omnata_plugin_runtime.omnata_plugin import (
    OmnataPlugin,
    PluginManifest,
    ConnectResponse,
    OutboundSyncRequest,
    InboundSyncRequest,
    StreamConfiguration,
    jinja_filter,
    managed_outbound_processing,
)
from omnata_plugin_runtime.configuration import (
    ConnectionConfigurationParameters,
    SendSyncStrategy,
    OutboundSyncConfigurationParameters,
    InboundSyncConfigurationParameters,
    InboundSyncStrategy,
)
from omnata_plugin_runtime.rate_limiting import ApiLimits, HttpRequestMatcher, RequestRateLimit
from slack_sdk import WebClient
from slack_sdk.web.base_client import SlackResponse

logger = getLogger(__name__)


class SlackPlugin(OmnataPlugin):
    """
    An example plugin which posts messages to Slack, and retrieves the list of channels.
    """

    def __init__(self):
        OmnataPlugin.__init__(self)
        self.slack_user_email_cache = {}

    def get_manifest(self) -> PluginManifest:
        """
        Returns the Plugin Manifest, provides information about the plugin.
        """
        return PluginManifest(
            plugin_id="slack",
            plugin_name="Slack",
            developer_id="omnata",
            developer_name="Omnata",
            docs_url="https://docs.omnata.com",
            supports_inbound=True,
            supported_outbound_strategies=[SendSyncStrategy()],
        )

    def connection_form(self) -> List[ConnectionMethod]:
        """
        Returns information to populate the connection form.
        Slack supports OAuth, so we return an OAuth template which will guide the user through creation of a security integration.
        """
        return [
            ConnectionMethod(
                name="OAuth (User Created App)",
                fields=[],
                oauth_template=SecurityIntegrationTemplate(
                    oauth_client_id="<Client ID from App Credentials>",
                    oauth_client_secret="<Client Secret from App Credentials>",
                    oauth_token_endpoint="https://slack.com/api/oauth.v2.access",
                    oauth_authorization_endpoint="https://slack.com/oauth/v2/authorize",
                    oauth_allowed_scopes=["chat:write", "users:read.email", "users:read", "channels:read"],
                ),
            )
        ]

    def network_addresses(self, parameters: ConnectionConfigurationParameters) -> List[str]:
        """
        Returns the network addresses that the plugin will connect to.
        The user will be instructed to create a network rule to allow the plugin to connect to these addresses.
        """
        return ["slack.com", "www.slack.com"]

    def connect(self, parameters: ConnectionConfigurationParameters) -> ConnectResponse:
        """
        Tests the connection to Slack, using the provided parameters
        """
        self._get_webclient(parameters).auth_test()
        return ConnectResponse()

    def _get_webclient(self, parameters: SyncConfigurationParameters) -> WebClient:
        """
        Private function to get a Slack web client, using the provided parameters
        """
        access_token = parameters.get_connection_secret("access_token").value
        return WebClient(token=access_token)

    def api_limits(self, parameters: SyncConfigurationParameters) -> List[ApiLimits]:
        """
        Defines the API limits for the various parts of the Slack API.
        These are automatically enforced by the Omnata engine.
        """
        return [
            ApiLimits(
                endpoint_category="Web API Tier 1",
                request_matchers=[
                    HttpRequestMatcher(http_methods=["GET", "POST", "PUT"], url_regex="admin.teams.create")
                ],
                request_rates=[RequestRateLimit(request_count=5, time_unit="minute", unit_count=1)],
            ),
            ApiLimits(
                endpoint_category="Web API Tier 2",
                request_matchers=[
                    HttpRequestMatcher(http_methods=["GET", "POST", "PUT"], url_regex="conversations.list")
                ],
                request_rates=[RequestRateLimit(request_count=25, time_unit="minute", unit_count=1)],
            ),
            ApiLimits(
                endpoint_category="Web API Tier 3",
                request_matchers=[
                    HttpRequestMatcher(http_methods=["GET", "POST", "PUT"], url_regex="conversations.history")
                ],
                request_rates=[RequestRateLimit(request_count=55, time_unit="minute", unit_count=1)],
            ),
            ApiLimits(
                endpoint_category="Web API Tier 4",
                request_matchers=[HttpRequestMatcher(http_methods=["GET", "POST", "PUT"], url_regex="users.info")],
                request_rates=[RequestRateLimit(request_count=105, time_unit="minute", unit_count=1)],
            ),
            ApiLimits(
                endpoint_category="Posting messages",
                request_matchers=[
                    HttpRequestMatcher(http_methods=["GET", "POST", "PUT"], url_regex="chat.postMessage")
                ],
                request_rates=[RequestRateLimit(request_count=1, time_unit="second", unit_count=1)],
            ),
        ]

    def outbound_configuration_form(self, parameters: OutboundSyncConfigurationParameters) -> OutboundSyncConfigurationForm:
        """
        Returns a form which will be presented to the user when configuring the outbound sync.
        """
        return OutboundSyncConfigurationForm(
            fields=[
                FormDropdownField(
                    name="channel",
                    label="Channel to post in",
                    help_text="If the channel is not visible out, you need to invite the bot user to the channel",
                    required=True,
                    data_source=DynamicFormOptionsDataSource(source_function=self._fetch_channel_list),
                )
            ],
            mapper=FormJinjaTemplate(label="Message Template"),
        )

    def _fetch_channel_list(self, parameters: OutboundSyncConfigurationParameters) -> List[FormOption]:
        """
        Populates the channel dropdown with the list of channels that the bot user is a member of.
        """
        slack_client = self._get_webclient(parameters)
        response = slack_client.conversations_list()
        self._raise_error_if_not_ok(response)
        fields = []
        for channel in response.data["channels"]:
            if channel["is_member"]:
                fields.append(
                    FormOption(
                        value=channel["name"], label="#" + channel["name"], data_type_icon="text", metadata=channel
                    )
                )
        return sorted(fields, key=lambda d: d["label"])

    def sync_outbound(self, parameters: OutboundSyncConfigurationParameters, outbound_sync_request: OutboundSyncRequest):
        """
        This method is called by the Omnata engine whenever an outbound sync is performed.
        """
        records = outbound_sync_request.get_records(batched=True)
        channel = parameters.get_sync_parameter("channel").value
        webclient = self._get_webclient(parameters)
        self.record_upload(data_frame=records,channel=channel,webclient=webclient)

    @managed_outbound_processing(concurrency=2, batch_size=1)
    def record_upload(self, data_frame: pandas.DataFrame, channel: str, webclient: WebClient) -> pandas.DataFrame:
        """
        Using the @managed_outbound_processing decorator, this function will be invoked in parallel, with a controlled batch size,
        at a controlled rate according to the sync configuration.
        """
        load_records_frame = None
        logger.info(f"record_upload given {len(data_frame)} records")
        for index, row in data_frame.iterrows():
            transformed_record = row["TRANSFORMED_RECORD"]
            response = webclient.chat_postMessage(channel=f"#{channel}", text=transformed_record)
            self._raise_error_if_not_ok(response)
            response_df = pandas.DataFrame(
                [
                    {
                        "IDENTIFIER": row["IDENTIFIER"],
                        "APP_IDENTIFIER": None,
                        "SUCCESS": response.data["ok"],
                        "RESULT": response.data,
                    }
                ]
            )
            load_records_frame = pandas.concat([load_records_frame, response_df])
        return load_records_frame

    @jinja_filter
    def lookup_slack_user_by_email(self, value):
        """
        Looks up a Slack user's ID by email address.
        This is a jinja filter which, if defined in the template, is invoked by the Omnata engine during preparation of the outbound sync.
        """
        if value in self.slack_user_email_cache:
            return self.slack_user_email_cache[value]
        logger.info(f"looking up user by email {value}")
        # we need to access the configuration parameters from within a jinja filter, we can use the _configuration_parameters attribute
        response = self._get_webclient(self._configuration_parameters).users_lookupByEmail(email=value)
        self._raise_error_if_not_ok(response)
        user_id = response["user"]["id"]
        logger.info(f"storing ID {user_id} in cache, for email {value}")
        self.slack_user_email_cache[value] = user_id
        return user_id
    
    def inbound_configuration_form(self, parameters: InboundSyncConfigurationParameters) -> InboundSyncConfigurationForm:
        """
        Returns a form which will be presented to the user when configuring the inbound sync.
        There are no fields to configure for inbound syncs in this plugin.
        """
        return InboundSyncConfigurationForm(fields=[])

    def inbound_stream_list(self, parameters: InboundSyncConfigurationParameters) -> List[StreamConfiguration]:
        """
        Lists the streams available for inbound syncs.
        Currently we just offer the conversations_list stream, which returns a list of channels that the bot user is a member of.
        Only FULL_REFRESH is offered, because it's typically a relatively small list and not worth fetching incrementally.
        """
        streams_to_return: List[StreamConfiguration] = []
        streams_to_return.append(
            StreamConfiguration(
                stream_name="conversations_list",
                supported_sync_strategies=[InboundSyncStrategy.FULL_REFRESH],
                source_defined_cursor=True,
                default_cursor_field="updated",
                source_defined_primary_key="id",
                json_schema=CHANNEL_SCHEMA,
            )
        )
        return streams_to_return

    def sync_inbound(self, parameters: InboundSyncConfigurationParameters, inbound_sync_request: InboundSyncRequest):
        """
        This method is called by the Omnata engine whenever an inbound sync is performed.
        We don't use the @managed_inbound_processing decorator here, because there's only a single stream.
        """
        for stream in inbound_sync_request.streams:
            if stream.stream.stream_name == "conversations_list":
                response = self._get_webclient(parameters).conversations_list()
                self._raise_error_if_not_ok(response)
                # 'channels' contains a list of channels, these are the records we want to return
                inbound_sync_request.enqueue_results(stream.stream.stream_name, response["channels"], {})

    def _raise_error_if_not_ok(self, response: SlackResponse):
        """
        Raises an error if the Slack API response is not ok
        """
        if response["ok"] is False:
            raise ValueError(response["error"])



CHANNEL_SCHEMA = {
    "$schema": "http://json-schema.org/draft-06/schema#",
    "type": "object",
    "additionalProperties": True,
    "properties": {
        "id": {"type": "string"},
        "name": {"type": "string"},
        "is_channel": {"type": "boolean"},
        "is_group": {"type": "boolean"},
        "is_im": {"type": "boolean"},
        "created": {"type": "integer"},
        "creator": {"type": "string"},
        "is_archived": {"type": "boolean"},
        "is_general": {"type": "boolean"},
        "unlinked": {"type": "integer"},
        "name_normalized": {"type": "string"},
        "is_read_only": {"type": "boolean"},
        "is_shared": {"type": "boolean"},
        "is_ext_shared": {"type": "boolean"},
        "is_org_shared": {"type": "boolean"},
        "pending_shared": {"type": "array", "items": {}},
        "is_pending_ext_shared": {"type": "boolean"},
        "is_member": {"type": "boolean"},
        "is_private": {"type": "boolean"},
        "is_mpim": {"type": "boolean"},
        "last_read": {"type": "string"},
        "topic": {
            "type": "object",
            "additionalProperties": True,
            "properties": {"value": {"type": "string"}, "creator": {"type": "string"}, "last_set": {"type": "integer"}},
        },
        "purpose": {
            "type": "object",
            "additionalProperties": True,
            "properties": {"value": {"type": "string"}, "creator": {"type": "string"}, "last_set": {"type": "integer"}},
        },
        "previous_names": {"type": "array", "items": {"type": "string"}},
        "num_members": {"type": "integer"},
        "locale": {"type": "string"},
    },
}
