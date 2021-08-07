# clash

Works like singleflight, but caches the previously fetched data.


```go
	// Define a finder.
	userFinder := func(ctx context.Context, keys []clash.Key) ([]clash.Result, error) {
		ids := make([]string, len(keys))
		for i, key := range keys {
			ids[i] = key.String()
		}

		// This is commonly a find entity with SQL IN (...).
		users, err := m.findUsersByIDs(ctx, ids)
		if err != nil {
			return nil, err
		}
		result := make([]clash.Result, len(keys))
		for i, user := range users {
			// Set the result, can either be a value or error.
			result[i] = clash.Result{
				Value: user,
				Key:   clash.Key(user.ID),
				Error: nil,
			}
		}
		return result, nil
	}

	// Initialize a new dataloader.
	// This should ideally be per-request scope, pretty much like how dataloader
	// would work.
	l := clash.NewLoader(userFinder)
	defer l.Close()

	users := make([]User, n)


	// Fetch data concurrently.
	g := new(errgroup.Group)
	for i := 0; i < n; i++ {
		// Important, closure.
		i := i

		g.Go(func() error {
			ctx := context.Background()
			u, err := l.Load(ctx, clash.Key(fmt.Sprint(i)))
			if err != nil {
				return err
			}
			users[i], _ = u.(User)
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
```

## TODO
- load many
- set cache
