package testutils

// TestScenario provides predefined test scenarios for common use cases
type TestScenario struct {
	Name        string
	Description string
	FileSet     *StateFileSet
	DataSet     *TestDataSet
}

// NewTestScenario creates a new test scenario
func NewTestScenario(name, description, baseDir string) *TestScenario {
	return &TestScenario{
		Name:        name,
		Description: description,
		FileSet:     NewStateFileSet(baseDir),
		DataSet:     NewTestDataSet(3, 5), // Default: 3 users, 5 posts
	}
}

// CreateBasicSocialActivityScenario creates files simulating basic social activity
func (s *TestScenario) CreateBasicSocialActivityScenario() error {
	// Create hypersync files with initial state
	if err := s.CreateInitialHypersyncState(); err != nil {
		return err
	}

	// Create committed block files with new activity
	if err := s.CreateCommittedBlockActivity(); err != nil {
		return err
	}

	// Create mempool files with pending activity
	return s.CreateMempoolActivity()
}

// CreateInitialHypersyncState creates hypersync files with profiles and initial posts
func (s *TestScenario) CreateInitialHypersyncState() error {
	baseTime := uint64(1640995200000000000) // Fixed timestamp for consistent tests

	// Chunk 1: User profiles
	err := s.FileSet.CreateHypersyncFileWithBuilder(100, 1, baseTime, func(b *StateFileBuilder) *StateFileBuilder {
		for i, user := range s.DataSet.Users {
			if i < 2 { // Only first 2 users in chunk 1
				b.WithProfile(user.PKID, user.PublicKey, user.Username,
					string(user.Profile.Description), string(user.Profile.ProfilePic))
			}
		}
		return b
	})
	if err != nil {
		return err
	}

	// Chunk 2: Initial posts
	err = s.FileSet.CreateHypersyncFileWithBuilder(100, 2, baseTime, func(b *StateFileBuilder) *StateFileBuilder {
		for i, post := range s.DataSet.Posts {
			if i < 2 { // Only first 2 posts in chunk 2
				b.WithPost(post.Hash, post.Entry.PosterPublicKey, post.Body, post.TimestampNanos)
			}
		}
		return b
	})
	if err != nil {
		return err
	}

	// Chunk 3: Third user profile and a follow relationship
	return s.FileSet.CreateHypersyncFileWithBuilder(100, 3, baseTime, func(b *StateFileBuilder) *StateFileBuilder {
		if len(s.DataSet.Users) >= 3 {
			user := s.DataSet.Users[2]
			b.WithProfile(user.PKID, user.PublicKey, user.Username,
				string(user.Profile.Description), string(user.Profile.ProfilePic))

			// User 0 follows User 1
			if len(s.DataSet.Users) >= 2 {
				b.WithFollow(s.DataSet.Users[0].PKID, s.DataSet.Users[1].PKID)
			}
		}
		return b
	})
}

// CreateCommittedBlockActivity creates committed block files with new posts and interactions
func (s *TestScenario) CreateCommittedBlockActivity() error {
	// Block 101: New posts and profile update
	err := s.FileSet.CreateCommittedBlockFileWithBuilder(101, func(b *StateFileBuilder) *StateFileBuilder {
		if len(s.DataSet.Posts) >= 4 {
			// Add posts 3 and 4
			for i := 2; i < 4; i++ {
				post := s.DataSet.Posts[i]
				b.WithPost(post.Hash, post.Entry.PosterPublicKey, post.Body, post.TimestampNanos)
			}
		}

		// Update first user's profile
		if len(s.DataSet.Users) >= 1 {
			user := s.DataSet.Users[0]
			b.WithProfile(user.PKID, user.PublicKey, user.Username,
				"Updated profile description in block 101", string(user.Profile.ProfilePic))
		}

		return b
	})
	if err != nil {
		return err
	}

	// Block 102: More follows and likes
	return s.FileSet.CreateCommittedBlockFileWithBuilder(102, func(b *StateFileBuilder) *StateFileBuilder {
		if len(s.DataSet.Users) >= 3 && len(s.DataSet.Posts) >= 2 {
			// User 2 follows User 0
			b.WithFollow(s.DataSet.Users[2].PKID, s.DataSet.Users[0].PKID)

			// User 1 likes the first post
			b.WithLike(s.DataSet.Users[1].PublicKey, s.DataSet.Posts[0].Hash)

			// User 2 likes the second post
			b.WithLike(s.DataSet.Users[2].PublicKey, s.DataSet.Posts[1].Hash)
		}

		return b
	})
}

// CreateMempoolActivity creates mempool files with pending transactions
func (s *TestScenario) CreateMempoolActivity() error {
	baseTime := uint64(1640995300000000000)

	// Mempool file 1: New post
	err := s.FileSet.CreateMempoolFileWithBuilder(102, baseTime, func(b *StateFileBuilder) *StateFileBuilder {
		if len(s.DataSet.Posts) >= 5 {
			post := s.DataSet.Posts[4] // Fifth post
			b.WithPost(post.Hash, post.Entry.PosterPublicKey, post.Body, post.TimestampNanos)
		}
		return b
	})
	if err != nil {
		return err
	}

	// Create corresponding empty ancestral file
	if err := s.FileSet.CreateEmptyAncestralFile(102, baseTime); err != nil {
		return err
	}

	// Mempool file 2: Diamond and like
	err = s.FileSet.CreateMempoolFileWithBuilder(102, baseTime+1000000000, func(b *StateFileBuilder) *StateFileBuilder {
		if len(s.DataSet.Users) >= 2 && len(s.DataSet.Posts) >= 1 {
			// User 1 sends diamond to the first post (by User 0)
			b.WithDiamond(s.DataSet.Users[1].PKID, s.DataSet.Users[0].PKID,
				s.DataSet.Posts[0].Hash, 2)

			// User 0 likes their own post (unusual but valid for testing)
			b.WithLike(s.DataSet.Users[0].PublicKey, s.DataSet.Posts[0].Hash)
		}
		return b
	})
	if err != nil {
		return err
	}

	// Create corresponding empty ancestral file
	return s.FileSet.CreateEmptyAncestralFile(102, baseTime+1000000000)
}
