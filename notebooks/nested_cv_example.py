# NESTED CROSS-VALIDATION APPROACH
# Uses XGBoost's native CV functionality in a nested structure

import xgboost as xgb
import numpy as np
from sklearn.model_selection import KFold

# ============================================================================
# NESTED CV STRUCTURE
# ============================================================================
# Outer loop: 5 folds for unbiased performance estimation
# Inner loop: xgb.cv on each outer fold's training data for hyperparameter tuning
#
# Key: Each outer fold's test set is NEVER used during hyperparameter selection

y = dat['SP']

# Define outer CV splits (5-fold)
outer_cv = KFold(n_splits=5, shuffle=True, random_state=34)

# Hyperparameter grid
max_depth_grid = range(1,14,2)
min_child_weight_grid = [1,5,25,50,250]

# Store results
outer_fold_scores = []
outer_fold_best_params = []

# ============================================================================
# OUTER LOOP: Performance estimation
# ============================================================================
for fold_num, (train_val_idx, test_idx) in enumerate(outer_cv.split(X), 1):
    print("="*80)
    print(f"OUTER FOLD {fold_num}/5")
    print("="*80)
    
    # Split data for this outer fold
    X_train_val = X.iloc[train_val_idx]
    y_train_val = y.iloc[train_val_idx]
    X_test_outer = X.iloc[test_idx]
    y_test_outer = y.iloc[test_idx]
    
    # Create DMatrix for inner CV
    dtrain_val = xgb.DMatrix(X_train_val, label=y_train_val)
    
    # ========================================================================
    # INNER LOOP: Hyperparameter tuning using CV on train_val data only
    # ========================================================================
    best_params = None
    best_cv_score = float('inf')
    best_num_boost = 0
    
    for md in max_depth_grid:
        for mcw in min_child_weight_grid:
            params = {
                'max_depth': md,
                'min_child_weight': mcw,
                'gamma': 0,
                'eta': .01,
                'subsample': 1,
                'colsample_bytree': .8,
                'objective': 'reg:squarederror',
                'tree_method': 'exact',
                'seed': 34
            }
            
            # Run CV on the TRAINING portion of this outer fold
            cv_results = xgb.cv(
                params=params,
                dtrain=dtrain_val,
                num_boost_round=50000,
                early_stopping_rounds=250,
                nfold=4,  # inner CV folds
                verbose_eval=False,
                seed=34
            )
            
            # Get best score from inner CV
            min_rmse = cv_results['test-rmse-mean'].min()
            best_iter = cv_results['test-rmse-mean'].idxmin() + 1
            
            # Track best parameters for this outer fold
            if min_rmse < best_cv_score:
                best_cv_score = min_rmse
                best_params = params
                best_num_boost = best_iter
    
    print(f"\nBest params for outer fold {fold_num}:")
    print(f"  max_depth={best_params['max_depth']}, min_child_weight={best_params['min_child_weight']}")
    print(f"  Inner CV RMSE: {best_cv_score:.5f}")
    print(f"  Best iterations: {best_num_boost}")
    
    # ========================================================================
    # Train final model on full train_val set with best params
    # Evaluate on outer fold's test set (never seen during tuning)
    # ========================================================================
    dtest_outer = xgb.DMatrix(X_test_outer, label=y_test_outer)
    
    final_model = xgb.train(
        params=best_params,
        dtrain=dtrain_val,
        num_boost_round=best_num_boost,
        verbose_eval=False
    )
    
    # Evaluate on outer test fold
    outer_test_pred = final_model.predict(dtest_outer)
    outer_test_rmse = np.sqrt(np.mean((y_test_outer - outer_test_pred)**2))
    
    print(f"  Outer fold test RMSE: {outer_test_rmse:.5f}")
    print()
    
    outer_fold_scores.append(outer_test_rmse)
    outer_fold_best_params.append(best_params)

# ============================================================================
# FINAL RESULTS: Unbiased performance estimate
# ============================================================================
print("="*80)
print("NESTED CV RESULTS")
print("="*80)
print(f"Outer fold test RMSEs: {[f'{s:.5f}' for s in outer_fold_scores]}")
print(f"Mean outer fold RMSE: {np.mean(outer_fold_scores):.5f} ± {np.std(outer_fold_scores):.5f}")
print()
print("This is your UNBIASED estimate of generalization performance.")
print("="*80)

# ============================================================================
# OPTIONAL: Train final model on ALL data
# ============================================================================
# After nested CV, you can train a final model on all data using the most
# common hyperparameters selected across outer folds, or re-run tuning on all data

print("\nMost common hyperparameters across outer folds:")
from collections import Counter
param_tuples = [(p['max_depth'], p['min_child_weight']) for p in outer_fold_best_params]
most_common = Counter(param_tuples).most_common(1)[0]
print(f"  max_depth={most_common[0][0]}, min_child_weight={most_common[0][1]}")
print(f"  Selected in {most_common[1]}/5 outer folds")

# ============================================================================
# KEY POINTS ABOUT NESTED CV:
# ============================================================================
#
# 1. OUTER LOOP gives you an unbiased performance estimate
#    - Each outer test fold is never used during hyperparameter selection
#    - The mean outer fold RMSE is your honest generalization estimate
#
# 2. INNER LOOP does hyperparameter tuning
#    - Uses xgb.cv on the training portion of each outer fold
#    - Different hyperparameters may be selected for different outer folds
#
# 3. COMPUTATIONAL COST is high
#    - 5 outer folds × 35 param combinations × 4 inner CV folds = 700 model fits
#    - This is why nested CV is expensive but rigorous
#
# 4. COMPARISON to your current approach:
#    - Your approach: tune on all train data → test on held-out set
#      Problem: CV error is optimistically biased from extensive search
#    - Nested CV: tune separately for each outer fold → average outer test errors
#      Benefit: outer test errors are unbiased (never used for tuning)
#
# 5. WHAT TO REPORT:
#    - Report the mean outer fold RMSE as your model's expected performance
#    - This is more honest than reporting the best CV score from grid search
#
# 6. AFTER NESTED CV:
#    - You've estimated performance, but don't have a final model yet
#    - Option A: Use most common hyperparameters and train on all data
#    - Option B: Re-run tuning on all data (but know the CV score will be biased)
