from dataclasses import dataclass

@dataclass
class TypeInfo:
	account_address: str
	module_name: hex
	struct_name: hex

@dataclass
class TokenDataId:
	creator: str
	collection: str
	name: str

@dataclass
class TokenId:
	token_data_id: TokenDataId
	property_version: int

@dataclass
class TokenStakeEvent:
	token_id: TokenId
	initial_lockup_timestamp: int
	end_lockup_period: int
	coin_type_info: TypeInfo

@dataclass
class TokenUnstakeEvent:
	token_id: TokenId
	initial_lockup_timestamp: int
	stake_periods_accumulated: int
	coin_type_info: TypeInfo

@dataclass
class DistributeRewardEvent:
	token_id: TokenId
	reward: int
	coin_type_info: TypeInfo

def sort_stake_events(event):
	# token stake always has to come last in the sorted stake events, because there's a lockup period afterwards
	order = {
		'TokenStakeEvent': 3,
		'TokenUnstakeEvent': 2,
		'DistributeRewardEvent': 1
	}

	# default to processing other events after staking events
	return order.get(event.type.split('::')[-1], 999999999)

def sort_all_stake_events(event):
    # token stake always has to come last in the sorted stake events, because there's a lockup period afterwards
    order = {
        'TokenStakeEvent': 3,
        'TokenUnstakeEvent': 2,
        'DistributeRewardEvent': 1
    }

    # return a tuple with two elements: transaction version and event type sorting
    return (event['transaction_version'], order.get(event['type'].split('::')[-1], 999999999))