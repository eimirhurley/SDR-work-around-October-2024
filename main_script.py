import sys
import json
import requests
import math
import numpy as np
import pandas as pd


def main(token):
    URL = "https://8qh04u51x4.execute-api.eu-central-1.amazonaws.com/prod/sync_async"
    body = {
        "method": "getRepeatingSetRowsV2",
        "accessTokenStr": token,
        "projectId": 8729,
        "set": {"setName": "chqol"},
    }
    res = requests.post(url=URL, json=body)
    df = pd.DataFrame(json.loads(res.text)["dataRows"])

    # Define the groups created from the columns and their corresponding columns
    groups = {
        "AS": [
            "abilitytogetadvicefrompaediatriciant",
            "accesstocommunityservicesfacilitiest",
            "accesstoextrahelpwithlearningschoolt",
            "accesstorespitecaret",
            "amountofrespitecaret",
            "childsaccesstospecialistsurgerymedicalrxt",
            "childsaccesstotherapyt",
            "childsaccesstotreatmentt",
            "feelabouttheirspecialequipmenthomet",
            "feelabouttheirspecialequipmentschoolt",
            "feelabouttheirspecialequipmtincommunityt",
            "howeasytogetrespitecaret",
        ],
        "EW": [
            "feelabouttheirfuturet",
            "feelaboutthemselvest",
            "getalongwithyout",
            "howhappyischildt",
            "lifeingeneralt",
            "thewaytheylookt",
        ],
        "FAF": [
            "abilitytodressthemselvest",
            "abilitytoeatdrinkindependentlyt",
            "abilitytokeepupacademicallywithpeerst",
            "abilitytoplayalonet",
            "abilitytousetoiletindependentlyt",
            "communicatewithpeopledontknowwellt",
            "communicatewithpeopletheyknowwellt",
            "feelabouttheiropportunitiesinlifet",
            "howtheysleept",
            "otherscommunicatewiththemt",
            "thewaytheyusetheirarmst",
            "thewaytheyusetheirhandst",
        ],
        "FH": [
            "familiesfinancialsituationt",
            "howhappyareyout",
            "physicalhealthparentstiic",
            "worksituationt",
        ],
        "PAP": [
            "abilitytokeepupphysicallywithpeerst",
            "abilitytoparticipateatschoolt",
            "abilitytoparticipateincommunityt",
            "abilitytoparticipateinrecreationalactivityt",
            "abilitytoparticipateinsocialeventst",
            "abilitytoparticipateinsportt",
            "abilitytoplaywithfriendst",
            "abletodothingstheywanttot",
            "physicalhealthtxhb",
            "thewaytheyusetheirlegstwnu",
            "waytheygetaroundt",
        ],
        "PID": [
            "botheredbybeinghandledbyotherpeoplet",
            "botheredbyhospitalvisitst",
            "botheredwhenmissschoolforhealthreasonst",
            "concernedabouthavingcpt",
            "howdotheyfeelaboutamountofpaint",
            "howmuchdiscomfortt",
            "howmuchpaintlvg",
            "worryaboutwhowilltakecareoftheminfuturet",
        ],
        "SWA": [
            "acceptedbyadultst",
            "acceptedbyfamilyt",
            "acceptedbykidsoutsideschoolt",
            "acceptedbykidsschoolt",
            "acceptedbypeopleingeneralt",
            "getalongwithadultst",
            "getalongwithkidsoutsideschoolt",
            "getalongwithkidsschoolt",
            "getalongwithpeoplet",
            "getalongwithsiblingst",
            "getalongwithteacherscarerst",
            "goingoutontripswithfamilyt",
        ],
        # Add more groups as needed
    }
    # Calculate the average for each group and assign it to a new column
    for group_name, columns in groups.items():
        df[f"{group_name}_average"] = df[columns].mean(axis=1).round(1)

    URL = "https://td0zydpb43.execute-api.eu-central-1.amazonaws.com/prod/sync_async"

    def values_differ(val1, val2):
        if val1 is None and val2 is None:
            return False
        if (
            isinstance(val1, float)
            and math.isnan(val1)
            and isinstance(val2, float)
            and math.isnan(val2)
        ):
            return False
        return val1 != val2

    def is_valid_value(value):
        if value is None:
            return False
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            return False
        return True

    columns_to_check = [
        ("SWA_average", "socialwellbeingandacceptancecalc"),
        ("EW_average", "emotionalwellbeingselfesteemcalc"),
        ("FH_average", "familyhealthcalc"),
        ("FAF_average", "feelingsaboutfunctioningcalc"),
        ("PAP_average", "participationphysicalhealthcalc"),
        ("AS_average", "accesstoservicescalc"),
        ("PID_average", "painandimpactofdisabilitycalc"),
    ]

    for index, row in df.iterrows():
        observation_data = {}

        for avg_col, calc_col in columns_to_check:
            if avg_col in df.columns and calc_col in df.columns:
                if values_differ(row[avg_col], row[calc_col]):
                    observation_data[calc_col] = (
                        row[avg_col] if is_valid_value(row[avg_col]) else None
                    )

        if observation_data:  # Only make the API call if there's something to update
            body = {
                "method": "updateDataEntry",
                "accessTokenStr": token,
                "projectId": 8729,
                "datasetentryid": row["datasetentryid"],
                "observationData": observation_data,
                "set": {"setName": "chqol"},
            }
            try:
                res = requests.post(url=URL, json=body)
                print(
                    f"Updated entry {row['datasetentryid']} with values {observation_data}"
                )
            except requests.exceptions.RequestException as e:
                print(f"Failed to update entry {row['datasetentryid']}: {e}")
        else:
            print(
                f"Skipped entry {row['datasetentryid']} due to existing values or NaN in required fields"
            )

    # Ashworth
    URL = "https://8qh04u51x4.execute-api.eu-central-1.amazonaws.com/prod/sync_async"
    body = {
        "method": "getRepeatingSetRowsV2",
        "accessTokenStr": token,
        "projectId": 8729,
        "set": {"setName": "ashworth"},
    }
    res = requests.post(url=URL, json=body)
    df = pd.DataFrame(json.loads(res.text)["dataRows"])
    groups = {
        "LEFT": [
            "adductorsinflexionleft",
            "adductorsinneutralleft",
            "gastrocnemiusaleft",
            "hamstringsleft",
            "soleusaleft",
        ],
        "RIGHT": [
            "adductorsinflexionright",
            "adductorsinneutralright",
            "gastrocnemiusaright",
            "hamstringsright",
            "soleusaright",
        ],
        # Add more groups as needed
    }
    # Calculate the average for each group and assign it to a new column
    for group_name, columns in groups.items():
        df[f"{group_name}_average"] = df[columns].mean(axis=1).round(1)

    URL = "https://td0zydpb43.execute-api.eu-central-1.amazonaws.com/prod/sync_async"

    def values_differ(val1, val2):
        if val1 is None and val2 is None:
            return False
        if (
            isinstance(val1, float)
            and math.isnan(val1)
            and isinstance(val2, float)
            and math.isnan(val2)
        ):
            return False
        return val1 != val2

    def is_valid_value(value):
        if value is None:
            return False
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            return False
        return True

    columns_to_check = [
        ("LEFT_average", "ashworthaverageleft"),
        ("RIGHT_average", "ashworthaverageright"),
    ]

    for index, row in df.iterrows():
        observation_data = {}

        for avg_col, calc_col in columns_to_check:
            if avg_col in df.columns and calc_col in df.columns:
                if values_differ(row[avg_col], row[calc_col]):
                    observation_data[calc_col] = (
                        row[avg_col] if is_valid_value(row[avg_col]) else None
                    )

        if observation_data:  # Only make the API call if there's something to update
            left_val = observation_data.get(
                "ashworthaverageleft", row.get("ashworthaverageleft")
            )
            right_val = observation_data.get(
                "ashworthaverageright", row.get("ashworthaverageright")
            )

            valid_left = is_valid_value(left_val)
            valid_right = is_valid_value(right_val)

            if valid_left and valid_right:
                observation_data["ashworthaveragebothlegs"] = round(
                    (left_val + right_val) / 2, 1
                )
            elif valid_left:
                observation_data["ashworthaveragebothlegs"] = round(left_val, 1)
            elif valid_right:
                observation_data["ashworthaveragebothlegs"] = round(right_val, 1)
            body = {
                "method": "updateDataEntry",
                "accessTokenStr": token,
                "projectId": 8729,
                "datasetentryid": row["datasetentryid"],
                "observationData": observation_data,
                "set": {"setName": "ashworth"},
            }
            try:
                res = requests.post(url=URL, json=body)
                print(
                    f"Updated entry {row['datasetentryid']} with values {observation_data}"
                )
            except requests.exceptions.RequestException as e:
                print(f"Failed to update entry {row['datasetentryid']}: {e}")
        else:
            print(
                f"Skipped entry {row['datasetentryid']} due to existing values or NaN in required fields"
            )


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python main_script.py <token>")
        sys.exit(1)
    main(sys.argv[1])
