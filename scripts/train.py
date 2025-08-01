import duckdb
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from joblib import dump
from pathlib import Path
import pandas as pd
import os

PROJECT_DIR = Path(__file__).parent.parent.resolve()
DATA_DIR = PROJECT_DIR / 'data'
DB = DATA_DIR / 'cleaned' / 'risklens.duckdb'
MODEL_DIR = PROJECT_DIR / 'model'

RANDOM_STATE = 12345

def get_data():
    con = duckdb.connect(database=DB, read_only=True)
    
    # Load full dataset
    df = con.execute('select * from main_features_core.loan_acceptance_dataset').df()
    print(f'Original columns: {df.columns.tolist()}')
    print("Original class counts:")
    print(df['was_accepted'].value_counts())

    # Split by class
    accepted = df[df['was_accepted'] == 1]
    rejected = df[df['was_accepted'] == 0]

    # Get the minority count
    n = min(len(accepted), len(rejected))
    print(f"Balancing to {n} samples per class")

    # Sample equal counts from each
    accepted_bal = accepted.sample(n=n, random_state=RANDOM_STATE)
    rejected_bal = rejected.sample(n=n, random_state=RANDOM_STATE)

    # Combine & shuffle
    balanced_df = pd.concat([accepted_bal, rejected_bal]).sample(frac=1, random_state=RANDOM_STATE).reset_index(drop=True)

    print("Balanced class counts:")
    print(balanced_df['was_accepted'].value_counts())
    print(f"Balanced total rows: {len(balanced_df)}")

    # Optional: limit for fast testing
    max_rows = 10000
    if len(balanced_df) > max_rows:
        balanced_df = balanced_df.sample(n=max_rows, random_state=RANDOM_STATE).reset_index(drop=True)
        print(f"Truncated to {max_rows} rows for training speed.")

    return balanced_df

def prepare_model_input(df, feature_cols):
    numeric_feats = [col for col in feature_cols if df[col].dtype in ('int64', 'float64')]
    cat_feats = [col for col in feature_cols if df[col].dtype == 'object']
    preproc = ColumnTransformer(
        [('num', 'passthrough', numeric_feats),
         ('cat', OneHotEncoder(handle_unknown='ignore', sparse_output=False), cat_feats)],
         verbose_feature_names_out=False
    )
    X = preproc.fit_transform(df[feature_cols])
    print("Preprocessor expected features:", preproc.feature_names_in_)
    print("Input df columns:", df[feature_cols].columns.tolist())

    return X, preproc

def train_model(df, feature_cols, target_col, model_path):
    X, preproc = prepare_model_input(df, feature_cols)
    y = df[target_col]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=RANDOM_STATE)
    model = RandomForestClassifier(n_estimators=100, random_state=RANDOM_STATE)
    model.fit(X_train, y_train)
    print("Train Accuracy:", model.score(X_train, y_train))
    print("Test split:", y_test.value_counts())
    print("Train split:", y_train.value_counts())
    model_path.parent.mkdir(parents=True, exist_ok=True)
    dump((preproc, model), model_path)
    print(f'Trained and saved model for {target_col} at {model_path}')

if __name__ == '__main__':
    print('Training model')
    input_data = get_data()
    features = [
        'loan_amount', 'employment_length', 'annual_income',
        'income_to_loan_ratio', 'fico_avg',
        'int_rate_pct', 'home_ownership_cat',
        'purpose_debt_consolidation', 'purpose_credit_card',
        'purpose_home_improvement', 'purpose_major_purchase',
        'purpose_other',
        'term_months'
    ]

    # Train acceptance model
    train_model(input_data, features, 'was_accepted', MODEL_DIR / 'acceptance_model.joblib')

    # Train repayment model only on accepted loans
    df_accepted = input_data[input_data['was_accepted'] == 1]
    train_model(df_accepted, features, 'loan_repaid_binary', MODEL_DIR / 'repaid_model.joblib')
