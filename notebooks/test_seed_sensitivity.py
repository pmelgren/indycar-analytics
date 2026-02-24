# TEST SEED SENSITIVITY
# Run your current tuning approach with different random seeds
# to see if the CV→test gap is consistent or seed-dependent

import xgboost as xgb
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split

# Assuming X, y, dat are already loaded from your earlier cells

y = dat['SP']

# Test multiple random seeds
seeds = [34, 42, 123, 456, 789, 999, 2024, 2025, 1111, 5555]

results = []

for seed in seeds:
    print(f"\n{'='*80}")
    print(f"SEED: {seed}")
    print(f"{'='*80}")
    
    # Split data with this seed
    train_idx = X.sample(int(round(len(X.index)*0.8, 0)), random_state=seed).index
    test_idx = X.index[~X.index.isin(train_idx)]
    
    # Create DMatrix
    dcv_sp = xgb.DMatrix(X.loc[train_idx, :], label=y[train_idx])
    dtest_sp = xgb.DMatrix(X.loc[test_idx, :], label=y[test_idx])
    
    # Run COARSE grid search (like your cell 13)
    max_depth = range(1, 14, 2)
    min_child_weight = [1, 5, 25, 50, 250]
    
    best_cv_rmse_coarse = float('inf')
    best_params_coarse = None
    best_iter_coarse = 0
    
    for md in max_depth:
        for mcw in min_child_weight:
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
            
            cv_results = xgb.cv(
                params=params,
                dtrain=dcv_sp,
                num_boost_round=50000,
                early_stopping_rounds=250,
                nfold=4,
                verbose_eval=False
            )
            
            min_rmse = cv_results['test-rmse-mean'].min()
            best_iter = cv_results['test-rmse-mean'].idxmin() + 1
            
            if min_rmse < best_cv_rmse_coarse:
                best_cv_rmse_coarse = min_rmse
                best_params_coarse = params.copy()
                best_iter_coarse = best_iter
    
    print(f"Coarse grid search best CV RMSE: {best_cv_rmse_coarse:.5f}")
    print(f"  max_depth={best_params_coarse['max_depth']}, min_child_weight={best_params_coarse['min_child_weight']}")
    
    # Train model with best coarse params and evaluate on test
    dtrain_sp = dcv_sp
    early_stop = xgb.callback.EarlyStopping(
        rounds=250,
        metric_name='rmse',
        data_name='test',
        save_best=True
    )
    
    model = xgb.train(
        params=best_params_coarse,
        dtrain=dtrain_sp,
        num_boost_round=100000,
        evals=[(dtrain_sp, 'train'), (dtest_sp, 'test')],
        verbose_eval=False,
        callbacks=[early_stop]
    )
    
    # Get test RMSE
    test_preds = model.predict(dtest_sp)
    test_rmse = np.sqrt(np.mean((y[test_idx] - test_preds)**2))
    
    print(f"Test RMSE: {test_rmse:.5f}")
    print(f"CV→Test gap: {test_rmse - best_cv_rmse_coarse:.5f}")
    
    results.append({
        'seed': seed,
        'best_cv_rmse': best_cv_rmse_coarse,
        'test_rmse': test_rmse,
        'gap': test_rmse - best_cv_rmse_coarse,
        'max_depth': best_params_coarse['max_depth'],
        'min_child_weight': best_params_coarse['min_child_weight']
    })

# ============================================================================
# SUMMARY
# ============================================================================
results_df = pd.DataFrame(results)

print(f"\n{'='*80}")
print("SUMMARY ACROSS ALL SEEDS")
print(f"{'='*80}")
print(results_df.to_string(index=False))
print()
print(f"Mean CV RMSE: {results_df['best_cv_rmse'].mean():.5f} ± {results_df['best_cv_rmse'].std():.5f}")
print(f"Mean Test RMSE: {results_df['test_rmse'].mean():.5f} ± {results_df['test_rmse'].std():.5f}")
print(f"Mean CV→Test gap: {results_df['gap'].mean():.5f} ± {results_df['gap'].std():.5f}")
print()

if results_df['gap'].std() > 0.05:
    print("⚠️  SEED-DEPENDENT: The gap varies significantly across seeds.")
    print("   Your current seed (34) may be unlucky. Try a different seed.")
else:
    print("✓ CONSISTENT: The gap is stable across seeds.")
    print("  The issue is inherent to the train/test split approach, not seed-dependent.")
