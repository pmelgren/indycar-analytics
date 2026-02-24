# TRAIN/VALIDATION/TEST SPLIT APPROACH
# This replaces the CV-based hyperparameter tuning in cell 13

# Instead of your current approach:
# - Train (80%) with CV for tuning → Test (20%)
#
# Use this approach:
# - Train (60%) for fitting → Validation (20%) for tuning → Test (20%) for final evaluation

import xgboost as xgb
from sklearn.model_selection import train_test_split

# ============================================================================
# STEP 1: Split data into train/val/test
# ============================================================================

# First split: separate out test set (20%)
train_val_idx, test_idx = train_test_split(X.index, test_size=0.2, random_state=34)

# Second split: split remaining into train (60% total) and validation (20% total)
train_idx, val_idx = train_test_split(train_val_idx, test_size=0.25, random_state=34)

print(f"Train size: {len(train_idx)} ({len(train_idx)/len(X):.1%})")
print(f"Validation size: {len(val_idx)} ({len(val_idx)/len(X):.1%})")
print(f"Test size: {len(test_idx)} ({len(test_idx)/len(X):.1%})")

# Create DMatrix objects
dtrain = xgb.DMatrix(X.loc[train_idx,:], label=y[train_idx])
dval = xgb.DMatrix(X.loc[val_idx,:], label=y[val_idx])
dtest = xgb.DMatrix(X.loc[test_idx,:], label=y[test_idx])

# ============================================================================
# STEP 2: Grid search using VALIDATION set (not CV)
# ============================================================================

y = dat['SP']

# set grid parameters
max_depth = range(1,14,2)
min_child_weight = [1,5,25,50,250]

# define output dictionary
tune_results = {'SP': {'params':[],'val_rmse':[],'best_iter':[]}}

# loop across all combinations
for md in max_depth:
    for mcw in min_child_weight:
        print("Max Depth: {} ; Min Child Weight: {}".format(md,mcw))
        
        # set parameters
        params = {'max_depth': md
                  ,'min_child_weight': mcw
                  ,'gamma': 0
                  ,'eta': .01 
                  ,'subsample': 1
                  ,'colsample_bytree': .8
                  ,'objective': 'reg:squarederror'
                  ,'tree_method': 'exact'
                  ,'seed': 34
                 }
        
        # Train on TRAIN set, evaluate on VALIDATION set
        # Use early stopping based on validation error
        evals_result = {}
        model = xgb.train(
            params=params,
            dtrain=dtrain,
            num_boost_round=50000,
            evals=[(dtrain, 'train'), (dval, 'validation')],
            early_stopping_rounds=250,
            verbose_eval=False,
            evals_result=evals_result
        )
        
        # Get best validation error
        best_iter = model.best_iteration
        val_rmse = evals_result['validation']['rmse'][best_iter]
        
        # Save results
        tune_results['SP']['params'].append(params)
        tune_results['SP']['val_rmse'].append(val_rmse)
        tune_results['SP']['best_iter'].append(best_iter + 1)
        
        print('Best Validation Error: {:.5f} in {} iterations.\n'.format(val_rmse, best_iter + 1))

# ============================================================================
# STEP 3: Select best parameters based on VALIDATION error
# ============================================================================

best_idx = tune_results['SP']['val_rmse'].index(min(tune_results['SP']['val_rmse']))
best_params = tune_results['SP']['params'][best_idx]
best_val_rmse = tune_results['SP']['val_rmse'][best_idx]

print("="*80)
print("BEST PARAMETERS (based on validation set):")
print(best_params)
print(f"Validation RMSE: {best_val_rmse:.5f}")
print("="*80)

# ============================================================================
# STEP 4: Train final model on TRAIN+VALIDATION, evaluate on TEST
# ============================================================================

# Combine train and validation for final training
dtrain_final = xgb.DMatrix(X.loc[train_val_idx,:], label=y[train_val_idx])

# Train with best parameters
final_model = xgb.train(
    params=best_params,
    dtrain=dtrain_final,
    num_boost_round=tune_results['SP']['best_iter'][best_idx],  # use same number of iterations
    evals=[(dtrain_final, 'train'), (dtest, 'test')],
    verbose_eval=100
)

# Get final test error
test_rmse = final_model.eval(dtest).split(':')[1]
print("\n" + "="*80)
print(f"FINAL TEST RMSE: {test_rmse}")
print("="*80)

# ============================================================================
# KEY DIFFERENCES FROM YOUR CURRENT APPROACH:
# ============================================================================
# 
# 1. NO CROSS-VALIDATION during tuning
#    - CV error is optimistically biased after testing many combinations
#    - Validation set error is a single, honest estimate
#
# 2. Validation set is ONLY used for hyperparameter selection
#    - Never used for training until the final model
#
# 3. Test set is COMPLETELY HELD OUT
#    - Only touched once at the very end
#    - Gives unbiased estimate of generalization
#
# 4. Final model trains on train+validation
#    - Uses all available data except test set
#    - Uses the number of iterations found during tuning
#
# EXPECTED RESULTS:
# - Validation error will be more realistic (closer to test error)
# - Less gap between validation and test error
# - More honest assessment of model performance
