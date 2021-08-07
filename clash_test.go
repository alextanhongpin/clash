package clash_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/alextanhongpin/clash"
	"golang.org/x/sync/errgroup"
)

func TestClash(t *testing.T) {
	validator := func(batch int64, ids []string) {
		switch batch {
		case 1:
			if len(ids) != 2 {
				m := make(map[string]int64)
				for _, id := range ids {
					m[id]++
				}
				if !(m["2"] == 1 && m["3"] == 1) {
					t.Fatalf("expected first batch to fetch users with id 2 and 3, got %v", ids)
				}
			}
		case 2:
			if len(ids) != 0 {
				t.Fatalf("expected second batch to load cached users, got %v", ids)
			}
		case 3:
			if !(len(ids) == 1 && ids[0] == "1") {
				t.Fatalf("expected third batch to fetch user with id 3, got %v", ids)
			}
		}
	}

	m := &mockRepository{
		validator: validator,
	}
	users, err := m.findUsers(context.Background(), 5)
	if err != nil {
		t.Fatalf("expected error to be nil, got %v", err)
	}
	if len(users) != 5 {
		t.Fatalf("expected to fetch 5 users, got %v", users)
	}
}

func TestClashFetchNone(t *testing.T) {
	validator := func(batch int64, ids []string) {
		switch batch {
		case 1:
			if len(ids) != 0 {
				t.Fatalf("expected fetch to be 0, got %v", ids)
			}
		}
	}

	m := &mockRepository{
		validator: validator,
	}
	users, err := m.findUsers(context.Background(), 0)
	if err != nil {
		t.Fatalf("expected error to be nil, got %v", err)
	}
	if len(users) != 0 {
		t.Fatalf("expected to fetch 0 users, got %v", users)
	}
}

func TestClashFetchCached(t *testing.T) {
	validator := func(batch int64, ids []string) {
		switch batch {
		case 1:
			if len(ids) != 0 {
				t.Fatalf("expected fetch to be 0, got %v", ids)
			}
		}
	}

	m := &mockRepository{
		validator: validator,
		cachedUser: &User{
			ID:   "2",
			Name: fmt.Sprintf("user-2"),
		},
	}
	users, err := m.findUsers(context.Background(), 1)
	if err != nil {
		t.Fatalf("expected error to be nil, got %v", err)
	}
	if len(users) != 1 {
		t.Fatalf("expected to fetch 1 user, got %v", users)
	}
}

func TestClashFetchBatchedNone(t *testing.T) {
	validator := func(batch int64, ids []string) {
		switch batch {
		case 1:
			if len(ids) != 0 {
				t.Fatalf("expected fetch to be 0, got %v", ids)
			}
		}
	}

	m := &mockRepository{
		validator: validator,
	}
	users, err := m.findBatchUsers(context.Background(), 0)
	if err != nil {
		t.Fatalf("expected error to be nil, got %v", err)
	}
	if len(users) != 0 {
		t.Fatalf("expected to fetch 5 users, got %v", users)
	}
}

func TestClashFetchBatchedUnique(t *testing.T) {
	validator := func(batch int64, ids []string) {
		switch batch {
		case 1:
			if len(ids) != 3 {
				t.Fatalf("expected fetch to be 3, got %v", ids)
			}
		}
	}

	m := &mockRepository{
		validator: validator,
	}
	users, err := m.findBatchUsers(context.Background(), 5)
	if err != nil {
		t.Fatalf("expected error to be nil, got %v", err)
	}
	if len(users) != 5 {
		t.Fatalf("expected to fetch 5 users, got %v", users)
	}
}

func TestClashFetchBatchedError(t *testing.T) {
	validator := func(batch int64, ids []string) {
		switch batch {
		case 1:
			if len(ids) != 0 {
				t.Fatalf("expected fetch to be 0, got %v", ids)
			}
		}
	}

	m := &mockRepository{
		validator: validator,
	}
	numUsers := 10
	users, err := m.findBatchUsersError(context.Background(), numUsers)
	if err == nil {
		t.Fatalf("expected error not to be nil, got %v", err)
	}
	if len(users) != 0 {
		t.Fatalf("expected to fetch 0 users, got %v", users)
	}
}

type User struct {
	ID   string
	Name string
}

type mockRepository struct {
	validator  func(batch int64, ids []string)
	count      int64
	cachedUser *User
}

func (m *mockRepository) findBatchUsersError(ctx context.Context, n int) ([]User, error) {
	l := clash.NewLoader(m.userFinderError)
	defer l.Close()

	users := make([]User, n)
	keys := make([]clash.Key, n)
	for i := 0; i < n; i++ {
		keys[i] = clash.Key(fmt.Sprint(m))
	}
	result, err := l.LoadMany(ctx, keys)
	if err != nil {
		return nil, err
	}
	for i, res := range result {
		if err := res.Error; err != nil {
			return nil, err
		}
		users[i] = res.Value.(User)
	}
	return users, nil
}

func (m *mockRepository) findBatchUsers(ctx context.Context, n int) ([]User, error) {
	l := clash.NewLoader(m.userFinder)
	defer l.Close()

	users := make([]User, n)
	keys := make([]clash.Key, n)
	for i := 0; i < n; i++ {

		var m int
		if i == 4 {
			m = 1
		} else if i%2 == 0 {
			m = 2
		} else {
			m = 3
		}
		keys[i] = clash.Key(fmt.Sprint(m))
	}
	result, err := l.LoadMany(ctx, keys)
	if err != nil {
		return nil, err
	}
	for i, res := range result {
		if err := res.Error; err != nil {
			return nil, err
		}
		users[i] = res.Value.(User)
	}
	return users, nil
}

func (m *mockRepository) findUsers(ctx context.Context, n int) ([]User, error) {
	l := clash.NewLoader(m.userFinder)
	defer l.Close()

	if m.cachedUser != nil {
		l.Set(ctx, clash.Result{
			Key:   clash.Key(m.cachedUser.ID),
			Value: *m.cachedUser,
			Error: nil,
		})
	}

	users := make([]User, n)

	g := new(errgroup.Group)
	for i := 0; i < n; i++ {
		// Important, closure.
		i := i

		g.Go(func() error {
			// This will demonstrate splitting into two batches.
			// The first batch will run after 16ms, and the second after 54ms.
			var duration time.Duration
			if i < 3 {
				duration = 10 * time.Millisecond
			} else {
				duration = 40 * time.Millisecond
			}
			time.Sleep(duration)

			var n int
			if i == 4 {
				n = 1
			} else if i%2 == 0 {
				n = 2
			} else {
				n = 3
			}
			// Fetch users in the sequence 2,3,2,3,1
			// In the first batch, only users 2, 3 will be fetched.
			// In the second batch, no fetches will be made because the result has been cached.
			// In the third batch, only 1 will be fetched.
			ctx := context.Background()
			u, err := l.Load(ctx, clash.Key(fmt.Sprint(n)))
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
	return users, nil
}

func (m *mockRepository) findUsersByIDs(ctx context.Context, ids []string) ([]User, error) {
	m.count++
	m.validator(m.count, ids)
	// In SQL, this will be an IN statement.
	users := make([]User, len(ids))
	for i, id := range ids {
		users[i] = User{
			ID:   id,
			Name: fmt.Sprintf("user-%d", i),
		}
	}
	return users, nil
}

func (m *mockRepository) userFinder(ctx context.Context, keys []clash.Key) ([]clash.Result, error) {
	ids := make([]string, len(keys))
	for i, key := range keys {
		ids[i] = key.String()
	}
	users, err := m.findUsersByIDs(ctx, ids)
	if err != nil {
		return nil, err
	}
	result := make([]clash.Result, len(keys))
	for i, user := range users {
		result[i] = clash.Result{
			Value: user,
			Key:   clash.Key(user.ID),
			Error: nil,
		}
	}
	return result, nil
}

func (m *mockRepository) userFinderError(ctx context.Context, keys []clash.Key) ([]clash.Result, error) {
	return nil, errors.New("something bad happened")
}
