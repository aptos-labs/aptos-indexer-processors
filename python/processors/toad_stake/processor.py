from aptos.transaction.v1 import transaction_pb2
from processors.toad_stake.models import ToadStakingUserData, Event
from typing import Dict, List
# from aptos.transaction.v1.transaction_pb2 import Event
from processors.toad_stake.event_types import (
    DistributeRewardEvent,
    TokenStakeEvent,
    TokenUnstakeEvent,
    sort_stake_events,
)
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

DEBUG: bool = False

class EventAndStakeData:
    def __init__(self, event: Event, transaction_version, transaction_timestamp, event_index, sequence_number):
        self._event = event
        self.transaction_version = transaction_version
        self.transaction_timestamp = transaction_timestamp
        self.event_index = event_index
        self.sequence_number = sequence_number

    def __getattr__(self, attr):
        # This method gets called if the attribute wasn't found the usual ways.
        # We delegate to the _event attribute here.
        return getattr(self._event, attr)

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
        event_db_objs: List[Event] = []
        for transaction in transactions:
            # Custom filtering
            # Here we filter out all transactions that are not of type TRANSACTION_TYPE_USER
            if (
                transaction.type
                != transaction_pb2.Transaction.TRANSACTION_TYPE_USER
            ):
                continue


            # Parse Transaction struct
            transaction_version = transaction.version
            transaction_block_height = transaction.block_height
            transaction_timestamp = general_utils.parse_pb_timestamp(
                transaction.timestamp
            )
            user_transaction = transaction.user
            sequence_number = user_transaction.request.sequence_number

            # Iterate over all events, throw away the ones we don't want
            for event_index, event in enumerate(user_transaction.events):
                # Skip events that don't match our filter criteria
                event_data = json.loads(event.data)
                parsed_tag = event.type_str.split("::")
                module_address = general_utils.standardize_address(parsed_tag[0])
                module_name = parsed_tag[1]
                event_type = parsed_tag[2]
                if not ToadStakeProcessor.included_event_type(
                    module_address, module_name, event_type
                ):
                    continue

                # event_db_objs.append(EventAndStakeData(event, transaction_version, transaction_timestamp, event_index, sequence_number))
        
            # if len(event_db_objs) > 0:
            #     ToadStakeProcessor.process_raw_events(event_db_objs)

                # Create an instance of Event
                event_db_obj = Event(
                    creation_number=event.key.creation_number,
                    sequence_number=sequence_number,
                    account_address=event.key.account_address,
                    transaction_version=transaction_version,
                    transaction_block_height=transaction_block_height,
                    type_str=event_type,
                    data=event.data,
                    transaction_timestamp=transaction_timestamp,
                    event_index=event_index,
                )
                event_db_objs.append(event_db_obj)

        self.insert_to_db(event_db_objs)

        return ProcessingResult(
            start_version=start_version,
            end_version=end_version,
        )
                
    def insert_to_db(self, parsed_objs: List[Event]) -> None:
        with Session() as session, session.begin():
            for obj in parsed_objs:
                session.merge(obj)

    @staticmethod
    def included_event_type(
        module_address: str,
        module_name: str,
        event_type: str,
    ) -> bool:
        correct_module_address = module_address == MODULE_ADDRESS
        correct_module_name = module_name == "steak"
        correct_event_type = event_type in [
            "TokenStakeEvent",
            "TokenUnstakeEvent",
            "DistributeRewardEvent",
        ]

        return correct_module_address and correct_module_name and correct_event_type

    @staticmethod
    def process_raw_events(event_db_objs: List[EventAndStakeData]) -> None:
        if len(event_db_objs) == 0:
            return
        parsed_objs: List[ToadStakingUserData] = []
        # Process stake events
        with Session() as session, session.begin():
            if any([obj.type_str.split('::')[-1] in ['TokenStakeEvent', 'TokenUnstakeEvent', 'DistributeRewardEvent'] for obj in event_db_objs]):
                sorted_events = sorted(event_db_objs, key=sort_stake_events)
                for event in sorted_events:
                    event_type = event.type_str.split('::')[-1]
                    event_data = None
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
                    user_data = session.get(
                        ToadStakingUserData, (event.key.account_address, toad_id)
                    )
                    row = ToadStakingUserData(
                        sequence_number=event.sequence_number,
                        creation_number = event.key.creation_number,
                        account_address=event.key.account_address,
                        transaction_version=event.transaction_version,
                        transaction_timestamp=event.transaction_timestamp,
                        toad_id=toad_id,
                        total_rewards_claimed=(user_data.total_rewards_claimed if user_data is not None else 0) + rewards_claimed,
                        last_event_type=event_type,
                        last_staking_initiated_timestamp=event.transaction_timestamp if event_type == 'TokenStakeEvent' else user_data.last_staking_initiated_timestamp,
                        last_rewards_claimed_timestamp=event.transaction_timestamp if event_type == 'DistributeRewardEvent' else None if (user_data is None) else (None if user_data.last_rewards_claimed_timestamp is None else user_data.last_rewards_claimed_timestamp),
                        last_unstaked_timestamp=event.transaction_timestamp if event_type == 'TokenUnstakeEvent' else None if (user_data is None) else (None if user_data.last_unstaked_timestamp is None else user_data.last_unstaked_timestamp),
                        is_staked=is_staked if is_staked is not None else user_data.is_staked,
                        inserted_at=event.transaction_timestamp,
                        event_index=event.event_index
                    )
                    if user_data is not None:
                        if DEBUG: print((user_data.total_rewards_claimed if user_data is not None else 0) + rewards_claimed)
                        if DEBUG: print(f'{user_data.account_address}: user_data.account_address')
                        if DEBUG: print(f'{user_data.transaction_version}: user_data.transaction_version')
                        if DEBUG: print(f'{user_data.toad_id}: user_data.toad_id')
                        if DEBUG: print(f'{user_data.total_rewards_claimed}: user_data.total_rewards_claimed')
                        if DEBUG: print(f'{user_data.last_event_type}: user_data.last_event_type')
                        if DEBUG: print(f'{user_data.last_staking_initiated_timestamp}: user_data.last_staking_initiated_timestamp')
                        if DEBUG: print(f'{user_data.last_rewards_claimed_timestamp}: user_data.last_rewards_claimed_timestamp')
                        if DEBUG: print(f'{user_data.last_unstaked_timestamp}: user_data.last_unstaked_timestamp')
                        if DEBUG: print(f'{user_data.is_staked}: user_data.is_staked')
                        if DEBUG: print(f'{event.key.account_address}, {event.transaction_version}, Aptoad #{toad_id}, inserted at {event.inserted_at} event type: {event_type.upper()} tx version {event.transaction_version} ')
                        # if event exists, merge/update it here
                    session.merge(
                        row
                    )
                    # else:
                        # otherwise, add all new events at end
                        # parsed_objs.append(row)
                    if DEBUG: print(f'last_staking_initiated_timestamp: {event.transaction_timestamp if event_type == "TokenStakeEvent" else user_data.last_staking_initiated_timestamp}')
                    if DEBUG: print(f'last_rewards_claimed_timestamp at {event.transaction_timestamp if event_type == "DistributeRewardEvent" else None if (user_data is None) else (None if user_data.last_rewards_claimed_timestamp is None else user_data.last_rewards_claimed_timestamp)}')
                    if DEBUG: print(f'last_unstaked_timestamp type: {event.transaction_timestamp if event_type == "TokenUnstakeEvent" else None if (user_data is None) else (None if user_data.last_unstaked_timestamp is None else user_data.last_unstaked_timestamp)}')
            # # Insert Events into database
            # sorted_events = sorted(parsed_objs, key=lambda event: event.transaction_version)
            # session.add_all(parsed_objs)
            # session.commit()

