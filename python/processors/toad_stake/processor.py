from aptos.transaction.v1 import transaction_pb2
from processors.toad_stake.models import ToadStakeEvent
from typing import Dict, List
from aptos.transaction.v1.transaction_pb2 import Event
from processors.toad_stake.event_types import DistributeRewardEvent, TokenStakeEvent, TokenUnstakeEvent
from utils.transactions_processor import ProcessingResult
from utils import general_utils
from utils.transactions_processor import TransactionsProcessor
from utils.models.schema_names import TOAD_STAKE_SCHEMA_NAME
from utils.session import Session
from utils.processor_name import ProcessorName
import json
from datetime import datetime

MODULE_ADDRESS = general_utils.standardize_address(
    "0x1f03a48de7dc0a076d275ebdeb0d80289b5a84a076dbf44fab69f38946de4115"
)

class ToadStakeProcessor(TransactionsProcessor):
    def name(self) -> str:
        return ProcessorName.TOAD_STAKE.value

    def schema(self) -> str:
        return TOAD_STAKE_SCHEMA_NAME

    def process_transactions(
        self,
        transactions: list[transaction_pb2.Transaction],
        start_version: int,
        end_version: int,
    ) -> ProcessingResult:
        
        with Session() as session, session.begin():
            event_db_objs: List[ToadStakeEvent] = []

            for transaction in transactions:
                # Custom filtering
                # Here we filter out all transactions that are not of type TRANSACTION_TYPE_USER
                if transaction.type != transaction_pb2.Transaction.TRANSACTION_TYPE_USER:
                    continue

                # Parse Transaction struct
                transaction_version = transaction.version
                transaction_block_height = transaction.block_height
                transaction_timestamp = general_utils.parse_pb_timestamp(
                    transaction.timestamp
                )
                user_transaction = transaction.user

                # Parse ToadStakeEvent struct
                for event_index, event in enumerate(user_transaction.events):
                    # Skip events that don't match our filter criteria
                    event_data = json.loads(event.data)
                    parsed_tag = event.type_str.split('::')
                    module_address = general_utils.standardize_address(
                        parsed_tag[0]
                    )
                    module_name = parsed_tag[1]
                    event_type = parsed_tag[2]
                    if not ToadStakeProcessor.included_event_type(event, event_data, module_address, module_name, event_type):
                        continue
                    input()
                    # make default values based off of the assumption that they're staking a new toad for the first time, so no row exists
                    rewards_claimed = 0
                    is_staked = None
                    if event_type.endswith('TokenStakeEvent'):
                        event_data = TokenStakeEvent(**json.loads(event.data))
                        is_staked = True
                    elif event_type.endswith('TokenUnstakeEvent'):
                        event_data = TokenUnstakeEvent(**json.loads(event.data))
                        is_staked = False
                    elif event_type.endswith('DistributeRewardEvent'):
                        event_data = DistributeRewardEvent(**json.loads(event.data))
                        rewards_claimed = int(event_data.reward)
                    else:
                        continue

                    toad_id = int(event_data.token_id['token_data_id']['name'].split('Aptoad #')[1])

                    creation_number = event.key.creation_number
                    sequence_number = event.sequence_number
                    account_address = general_utils.standardize_address(
                        event.key.account_address
                    )

                    # print(f'{event.account_address}, {event.transaction_version}, Aptoad #{toad_id}, inserted at {event.inserted_at} event type: {event_type.upper()} tx version {event.transaction_version} ')
                    user_data = session.get(
                        ToadStakeEvent, (event.account_address, toad_id)
                    )

                    # Create an instance of ToadStakeEvent
                    event_db_obj = ToadStakeEvent(
                        account_address=event.account_address,
                        transaction_version=event.transaction_version,
                        toad_id=toad_id,
                        total_rewards_claimed=(user_data.total_rewards_claimed if user_data is not None else 0) + rewards_claimed,
                        last_event_type=event_type,
                        last_staking_initiated_timestamp=event.inserted_at if event_type == 'TokenStakeEvent' else user_data.last_staking_initiated_timestamp,
                        last_rewards_claimed_timestamp=event.inserted_at if event_type == 'DistributeRewardEvent' else None if (user_data is None) else (None if user_data.last_rewards_claimed_timestamp is None else user_data.last_rewards_claimed_timestamp),
                        last_unstaked_timestamp=event.inserted_at if event_type == 'TokenUnstakeEvent' else None if (user_data is None) else (None if user_data.last_unstaked_timestamp is None else user_data.last_unstaked_timestamp),
                        is_staked=is_staked if is_staked is not None else user_data.is_staked,
                    )

                    event_db_objs.append(event_db_obj)

            for obj in event_db_objs:
                session.merge(obj)

        return ProcessingResult(
            start_version=start_version,
            end_version=end_version,
        )

    @staticmethod
    def included_event_type(event: Event, event_json: Dict[str, any], module_address: str, module_name: str, event_type: str) -> bool:
        correct_module_address = module_address == MODULE_ADDRESS
        correct_module_name = module_name == 'steak'
        correct_event_type = event_type in ['TokenStakeEvent', 'TokenUnstakeEvent', 'DistributeRewardEvent']

        # written like this to avoid undefineds
        if correct_module_address and correct_module_name:
            if correct_event_type:
                if not (event_json['token_id']['token_data_id']['collection'] == 'Aptos Toad Overload' and event_json['token_id']['token_data_id']['creator'] == '0x74b6b765f6710a0c24888643babfe337241ad1888a55e33ed86f389fe3f13f52'):
                    print(event_json['token_id']['token_data_id']['collection'] == 'Aptos Toad Overload')
                    print(event_json['token_id']['token_data_id']['creator'] == '0x74b6b765f6710a0c24888643babfe337241ad1888a55e33ed86f389fe3f13f52')
                    print('falseee')
                    return False
        else:
            print(event.version)
            print(module_address, module_name, event_type)
            print(correct_module_address, correct_module_name, correct_event_type)
            input()
            return correct_module_address and correct_module_name and correct_event_type
