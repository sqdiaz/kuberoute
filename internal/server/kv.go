package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"kuberoute/internal/cache"
	"kuberoute/internal/policy"
	"kuberoute/internal/queue"
)

type Options struct {
	Primary                  *clientv3.Client
	Secondary                *clientv3.Client
	Policy                   policy.Engine
	Cache                    *cache.Store
	Queue                    *queue.Queue
	MaxStaleness             time.Duration
	ReplicationBaseBackoff   time.Duration
	ReplicationMaxBackoff    time.Duration
	ReplicationArtificialLag time.Duration
	Logger                   *log.Logger
}

type KVServer struct {
	etcdserverpb.UnimplementedKVServer

	primary                  *clientv3.Client
	secondary                *clientv3.Client
	policy                   policy.Engine
	cache                    *cache.Store
	queue                    *queue.Queue
	maxStaleness             time.Duration
	replicationBaseBackoff   time.Duration
	replicationMaxBackoff    time.Duration
	replicationArtificialLag time.Duration
	logger                   *log.Logger
}

func NewKVServer(opts Options) *KVServer {
	logger := opts.Logger
	if logger == nil {
		logger = log.Default()
	}
	return &KVServer{
		primary:                  opts.Primary,
		secondary:                opts.Secondary,
		policy:                   opts.Policy,
		cache:                    opts.Cache,
		queue:                    opts.Queue,
		maxStaleness:             opts.MaxStaleness,
		replicationBaseBackoff:   opts.ReplicationBaseBackoff,
		replicationMaxBackoff:    opts.ReplicationMaxBackoff,
		replicationArtificialLag: opts.ReplicationArtificialLag,
		logger:                   logger,
	}
}

func (s *KVServer) Register(grpcServer *grpc.Server) {
	etcdserverpb.RegisterKVServer(grpcServer, s)
}

func (s *KVServer) Put(ctx context.Context, req *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
	if err := validatePut(req); err != nil {
		return nil, err
	}

	mode := s.policy.ModeForKey(string(req.Key))
	opts := []clientv3.OpOption{}
	if req.Lease != 0 {
		opts = append(opts, clientv3.WithLease(clientv3.LeaseID(req.Lease)))
	}
	if req.PrevKv {
		opts = append(opts, clientv3.WithPrevKV())
	}

	resp, err := s.primary.Put(ctx, string(req.Key), string(req.Value), opts...)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "primary put failed: %v", err)
	}

	header := copyHeader(resp.Header)
	s.cache.UpsertIfNewer(cache.Entry{
		Key:         string(req.Key),
		Value:       append([]byte(nil), req.Value...),
		Lease:       req.Lease,
		ModRevision: header.Revision,
		FetchedAt:   time.Now().UTC(),
	})

	replicationStatus := "n/a"
	if mode == policy.ModeEventual {
		replicationStatus = "queued"
		err = s.enqueueReplication(ctx, queue.Operation{
			Type:  queue.OpPut,
			Key:   append([]byte(nil), req.Key...),
			Value: append([]byte(nil), req.Value...),
			Lease: req.Lease,
		})
		if err != nil {
			replicationStatus = "degraded"
			s.logger.Printf("replication enqueue failed after committed primary put key=%q err=%v", string(req.Key), err)
		}
	}

	_ = grpc.SetHeader(ctx, metadata.Pairs(
		"x-kuberoute-mode", string(mode),
		"x-kuberoute-source", "primary",
		"x-kuberoute-staleness-ms", "0",
		"x-kuberoute-replication", replicationStatus,
	))
	s.logger.Printf("put key=%q mode=%s source=primary replication=%s", string(req.Key), mode, replicationStatus)

	return &etcdserverpb.PutResponse{
		Header: header,
		PrevKv: resp.PrevKv,
	}, nil
}

func (s *KVServer) Range(ctx context.Context, req *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	mode := s.policy.ModeForKey(string(req.Key))

	if useCacheForRange(req, mode) {
		if entry, ok := s.cache.Get(string(req.Key)); ok {
			staleness := time.Since(entry.FetchedAt)
			if staleness <= s.maxStaleness {
				_ = grpc.SetHeader(ctx, metadata.Pairs(
					"x-kuberoute-mode", string(mode),
					"x-kuberoute-source", "cache",
					"x-kuberoute-staleness-ms", fmt.Sprintf("%d", staleness.Milliseconds()),
					"x-kuberoute-replication", "n/a",
				))
				s.logger.Printf("range key=%q mode=%s source=cache staleness_ms=%d", string(req.Key), mode, staleness.Milliseconds())
				return &etcdserverpb.RangeResponse{
					Header: &etcdserverpb.ResponseHeader{
						Revision: entry.ModRevision,
					},
					Kvs: []*mvccpb.KeyValue{
						{
							Key:            []byte(entry.Key),
							Value:          append([]byte(nil), entry.Value...),
							CreateRevision: entry.CreateRevision,
							ModRevision:    entry.ModRevision,
							Version:        entry.Version,
							Lease:          entry.Lease,
						},
					},
					Count: 1,
				}, nil
			}
		}
	}

	getOpts := make([]clientv3.OpOption, 0, 12)
	if len(req.RangeEnd) > 0 {
		getOpts = append(getOpts, clientv3.WithRange(string(req.RangeEnd)))
	}
	if req.Limit > 0 {
		getOpts = append(getOpts, clientv3.WithLimit(req.Limit))
	}
	if req.Revision > 0 {
		getOpts = append(getOpts, clientv3.WithRev(req.Revision))
	}
	if req.SortOrder != etcdserverpb.RangeRequest_NONE {
		getOpts = append(getOpts, clientv3.WithSort(toSortTarget(req.SortTarget), toSortOrder(req.SortOrder)))
	}
	if req.Serializable {
		getOpts = append(getOpts, clientv3.WithSerializable())
	}
	if req.KeysOnly {
		getOpts = append(getOpts, clientv3.WithKeysOnly())
	}
	if req.CountOnly {
		getOpts = append(getOpts, clientv3.WithCountOnly())
	}
	if req.MinModRevision > 0 {
		getOpts = append(getOpts, clientv3.WithMinModRev(req.MinModRevision))
	}
	if req.MaxModRevision > 0 {
		getOpts = append(getOpts, clientv3.WithMaxModRev(req.MaxModRevision))
	}
	if req.MinCreateRevision > 0 {
		getOpts = append(getOpts, clientv3.WithMinCreateRev(req.MinCreateRevision))
	}
	if req.MaxCreateRevision > 0 {
		getOpts = append(getOpts, clientv3.WithMaxCreateRev(req.MaxCreateRevision))
	}

	resp, err := s.primary.Get(ctx, string(req.Key), getOpts...)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "primary range failed: %v", err)
	}

	now := time.Now().UTC()
	if isSimplePointRead(req) && len(resp.Kvs) == 1 {
		s.cache.UpsertIfNewer(cache.Entry{
			Key:            string(resp.Kvs[0].Key),
			Value:          append([]byte(nil), resp.Kvs[0].Value...),
			Lease:          resp.Kvs[0].Lease,
			CreateRevision: resp.Kvs[0].CreateRevision,
			ModRevision:    resp.Kvs[0].ModRevision,
			Version:        resp.Kvs[0].Version,
			FetchedAt:      now,
		})
	}

	_ = grpc.SetHeader(ctx, metadata.Pairs(
		"x-kuberoute-mode", string(mode),
		"x-kuberoute-source", "primary",
		"x-kuberoute-staleness-ms", "0",
		"x-kuberoute-replication", "n/a",
	))
	s.logger.Printf("range key=%q mode=%s source=primary count=%d", string(req.Key), mode, resp.Count)

	return &etcdserverpb.RangeResponse{
		Header: copyHeader(resp.Header),
		Kvs:    resp.Kvs,
		More:   resp.More,
		Count:  resp.Count,
	}, nil
}

func (s *KVServer) DeleteRange(ctx context.Context, req *etcdserverpb.DeleteRangeRequest) (*etcdserverpb.DeleteRangeResponse, error) {
	if err := validateDeleteRange(req); err != nil {
		return nil, err
	}
	mode := s.policy.ModeForKey(string(req.Key))

	deleteOpts := []clientv3.OpOption{}
	if len(req.RangeEnd) > 0 {
		deleteOpts = append(deleteOpts, clientv3.WithRange(string(req.RangeEnd)))
	}
	if req.PrevKv {
		deleteOpts = append(deleteOpts, clientv3.WithPrevKV())
	}

	resp, err := s.primary.Delete(ctx, string(req.Key), deleteOpts...)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "primary delete failed: %v", err)
	}

	if len(req.RangeEnd) == 0 {
		s.cache.Delete(string(req.Key))
	}

	replicationStatus := "n/a"
	if mode == policy.ModeEventual {
		replicationStatus = "queued"
		err = s.enqueueReplication(ctx, queue.Operation{
			Type:     queue.OpDelete,
			Key:      append([]byte(nil), req.Key...),
			RangeEnd: append([]byte(nil), req.RangeEnd...),
		})
		if err != nil {
			replicationStatus = "degraded"
			s.logger.Printf("replication enqueue failed after committed primary delete key=%q err=%v", string(req.Key), err)
		}
	}

	_ = grpc.SetHeader(ctx, metadata.Pairs(
		"x-kuberoute-mode", string(mode),
		"x-kuberoute-source", "primary",
		"x-kuberoute-staleness-ms", "0",
		"x-kuberoute-replication", replicationStatus,
	))
	s.logger.Printf("delete key=%q mode=%s source=primary deleted=%d replication=%s", string(req.Key), mode, resp.Deleted, replicationStatus)

	return &etcdserverpb.DeleteRangeResponse{
		Header:  copyHeader(resp.Header),
		Deleted: resp.Deleted,
		PrevKvs: resp.PrevKvs,
	}, nil
}

func (s *KVServer) Txn(context.Context, *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
	return nil, status.Error(codes.Unimplemented, "Txn is not implemented in this MVP")
}

func (s *KVServer) Compact(context.Context, *etcdserverpb.CompactionRequest) (*etcdserverpb.CompactionResponse, error) {
	return nil, status.Error(codes.Unimplemented, "Compact is not implemented in this MVP")
}

func (s *KVServer) RunReplicator(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		op, err := s.queue.Peek()
		if err != nil {
			if errors.Is(err, queue.ErrEmpty) {
				time.Sleep(200 * time.Millisecond)
				continue
			}
			s.logger.Printf("queue peek failed: %v", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		if err := s.applyOperation(ctx, op); err != nil {
			_ = s.queue.RecordFailure(op.ID, err.Error())
			backoff := s.retryBackoff(op.Attempts + 1)
			s.logger.Printf("replication failed op=%d attempts=%d retry_in=%s err=%v", op.ID, op.Attempts+1, backoff, err)
			time.Sleep(backoff)
			continue
		}
		if err := s.queue.MarkDone(op.ID); err != nil {
			s.logger.Printf("mark done failed op=%d err=%v", op.ID, err)
			time.Sleep(500 * time.Millisecond)
			continue
		}
	}
}

func (s *KVServer) applyOperation(ctx context.Context, op queue.Operation) error {
	if s.replicationArtificialLag > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(s.replicationArtificialLag):
		}
	}

	switch op.Type {
	case queue.OpPut:
		opts := []clientv3.OpOption{}
		if op.Lease != 0 {
			opts = append(opts, clientv3.WithLease(clientv3.LeaseID(op.Lease)))
		}
		_, err := s.secondary.Put(ctx, string(op.Key), string(op.Value), opts...)
		return err
	case queue.OpDelete:
		opts := []clientv3.OpOption{}
		if len(op.RangeEnd) > 0 {
			opts = append(opts, clientv3.WithRange(string(op.RangeEnd)))
		}
		_, err := s.secondary.Delete(ctx, string(op.Key), opts...)
		return err
	default:
		return fmt.Errorf("unknown operation type: %s", op.Type)
	}
}

func (s *KVServer) retryBackoff(attempt int) time.Duration {
	backoff := s.replicationBaseBackoff
	for i := 1; i < attempt; i++ {
		backoff *= 2
		if backoff >= s.replicationMaxBackoff {
			return s.replicationMaxBackoff
		}
	}
	return backoff
}

func (s *KVServer) enqueueReplication(ctx context.Context, op queue.Operation) error {
	var lastErr error
	backoff := 50 * time.Millisecond
	for attempt := 0; attempt < 4; attempt++ {
		if _, err := s.queue.Enqueue(op); err == nil {
			return nil
		} else {
			lastErr = err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
	}
	return lastErr
}

func validatePut(req *etcdserverpb.PutRequest) error {
	if len(req.Key) == 0 {
		return status.Error(codes.InvalidArgument, "key is required")
	}
	if req.IgnoreLease {
		return status.Error(codes.Unimplemented, "IgnoreLease is not implemented in this MVP")
	}
	if req.IgnoreValue {
		return status.Error(codes.Unimplemented, "IgnoreValue is not implemented in this MVP")
	}
	return nil
}

func validateDeleteRange(req *etcdserverpb.DeleteRangeRequest) error {
	if len(req.Key) == 0 {
		return status.Error(codes.InvalidArgument, "key is required")
	}
	return nil
}

func useCacheForRange(req *etcdserverpb.RangeRequest, mode policy.Mode) bool {
	if mode != policy.ModeBoundedStale && mode != policy.ModeEventual {
		return false
	}
	return isSimplePointRead(req)
}

func isSimplePointRead(req *etcdserverpb.RangeRequest) bool {
	return len(req.RangeEnd) == 0 &&
		req.Revision == 0 &&
		req.Limit == 0 &&
		!req.CountOnly &&
		req.SortOrder == etcdserverpb.RangeRequest_NONE &&
		req.SortTarget == etcdserverpb.RangeRequest_KEY &&
		!req.KeysOnly &&
		req.MinModRevision == 0 &&
		req.MaxModRevision == 0 &&
		req.MinCreateRevision == 0 &&
		req.MaxCreateRevision == 0
}

func copyHeader(hdr *etcdserverpb.ResponseHeader) *etcdserverpb.ResponseHeader {
	if hdr == nil {
		return &etcdserverpb.ResponseHeader{}
	}
	out := *hdr
	return &out
}

func toSortTarget(target etcdserverpb.RangeRequest_SortTarget) clientv3.SortTarget {
	switch target {
	case etcdserverpb.RangeRequest_CREATE:
		return clientv3.SortByCreateRevision
	case etcdserverpb.RangeRequest_MOD:
		return clientv3.SortByModRevision
	case etcdserverpb.RangeRequest_VALUE:
		return clientv3.SortByValue
	case etcdserverpb.RangeRequest_VERSION:
		return clientv3.SortByVersion
	default:
		return clientv3.SortByKey
	}
}

func toSortOrder(order etcdserverpb.RangeRequest_SortOrder) clientv3.SortOrder {
	switch order {
	case etcdserverpb.RangeRequest_ASCEND:
		return clientv3.SortAscend
	case etcdserverpb.RangeRequest_DESCEND:
		return clientv3.SortDescend
	default:
		return clientv3.SortNone
	}
}
