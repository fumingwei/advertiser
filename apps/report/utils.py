from apps.accounts.define import Medium


def update_counts(account_status, counts, count, status_mapping):
    if account_status in status_mapping:
        counts[status_mapping[account_status]] += 1
        count[status_mapping[account_status]] += 1
    else:
        counts['else'] += 1
        count['else'] += 1


def process_account(account, counts):
    medium = account.get('medium', '')
    account_status = account.get('account_status', '')

    if medium == Medium.META.value:
        counts['total'] += 1
        counts['meta']['total'] += 1
        update_counts(account_status, counts, counts['meta'], {
            "1": 'active',
            "2": 'frozen',
            "100": 'disabled',
            "101": 'disabled'
        })

    elif medium == Medium.GOOGLE.value:
        counts['total'] += 1
        counts['gg']['total'] += 1
        update_counts(account_status, counts, counts['gg'], {
            "ENABLED": 'active',
            "SUSPENDED": 'frozen',
            "CANCELED": 'disabled'
        })

    elif medium == Medium.TIKTOK.value:
        counts['total'] += 1
        counts['tt']['total'] += 1
        update_counts(account_status, counts, counts['tt'], {
            "ENABLED": 'active',
            "SUSPENDED": 'frozen',
            "CANCELED": 'disabled'
        })


def create_medium_data(medium_name, total, active, frozen, disabled, else_count):
    return {
        "medium": medium_name,
        "account_total": total,
        "active_account": active,
        "frozen_account": frozen,
        "disabled_account": disabled,
        "else_account": else_count
    }
