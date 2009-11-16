package AnyEvent::Memcached::Protocol::Text;
use Any::Moose;
use namespace::clean -except => qw(meta);

extends 'AnyEvent::Memcached::Protocol';

{
    my $generator = sub {
        my ($self, $cmd) = @_;
        my $memcached = $self->memcached;
    
        return sub {
            my ($key, $value, $cb) = @_;
            my $handle = $self->get_handle_for( $key );
        
            $value ||= 1;
            my @command = ($cmd => $key => $value);
            my $noreply = 0; # XXX - FIXME
            if ($noreply) {
                push @command, "noreply";
            }
            $handle->push_write(join(' ', @command) . "\r\n");

            if (! $noreply) {
                $handle->push_read(regex => qr{\r\n}, sub {
                    my $data = $_[1];
                    $data =~ /^(NOT_FOUND|\w+)\r\n/;
                    $cb->($1 eq 'NOT_FOUND' ? undef : $1) if $cb;
                    $memcached->drain_queue;
                });
            }
        }
    };

    sub _build_decr_cb {
        my $self = shift;
        $generator->($self, "decr");
    }

    sub _build_incr_cb {
        my $self = shift;
        $generator->($self, "incr");
    }
}

sub _build_delete_cb {
    my $self = shift;
    my $memcached = $self->memcached;

    return sub {
        my ($key, $noreply, $cb) = @_;
        my $handle = $self->get_handle_for( $key );

        my @command = (delete => $key);
        $noreply = 0; # XXX - FIXME
        if ($noreply) {
            push @command, "noreply";
        }
        $handle->push_write(join(' ', @command) . "\r\n");

        if (! $noreply) {
            $handle->push_read(regex => qr{\r\n}, sub {
                my $data = $_[1];
                my $success = $data =~ /^DELETED\r\n/;
                $cb->($success) if $cb;
                $memcached->drain_queue;
            });
        }
    };
}

sub _build_get_multi_cb {
    my $self = shift;
    my $memcached = $self->memcached;

    return sub {
        my ($keys, $cb, $cb_caller) = @_;
        my %rv;
        my $cv = AE::cv {
            $cb_caller->($cb, \%rv);
            $memcached->drain_queue;
        };

        if (scalar @$keys == 0) {
            $cv->send;
            return;
        }

        my $count = $memcached->get_server_count();
        my @keysinserver;
        foreach my $key (@$keys) {
            my $hash   = $memcached->hashing_algorithm->hash($key);
            my $i      = $hash % $count;
            my $handle = $memcached->get_handle( $memcached->get_server($i) );
            my $list = $keysinserver[ $i ];
            if (! $list) {
                $keysinserver[ $i ] = $list = [ $handle ];
            }
            push @$list, $key;
        }
   
        for my $i (0..$#keysinserver) {
            next unless $keysinserver[$i];
            my ($handle, @keylist) = @{$keysinserver[$i]};
            $cv->begin;
            $handle->push_write( "get @keylist\r\n" );
            my $code; $code = sub {
                my ($handle, $line) = @_;
                if ($line =~ /^END(?:\r\n)?$/) {
                    undef $code;
                    $cv->end;
                } elsif ($line =~ /^VALUE (\S+) (\S+) (\S+)(?: (\S+))?/)  {
                    my ($rkey, $rflags, $rsize, $rcas) = ($1, $2, $3, $4);
                    $handle->push_read(chunk => $rsize, sub {
                        my $data = $_[1];
                        if ($rflags & AnyEvent::Memcached::Protocol::F_COMPRESS() && AnyEvent::Memcached::Protocol::HAVE_ZLIB()) {
                            $data = Compress::Zlib::memGunzip($data);
                        }
                        if ($rflags & AnyEvent::Memcached::Protocol::F_STORABLE()) {
                            $data = Storable::thaw($data);
                        }
                        $rv{ $rkey } = $data; # XXX whatabout CAS?
                        $handle->push_read(regex => qr{\r\n}, cb => sub { "noop" });
                        $handle->push_read(line => $code);
                    } );
                } else {
                    confess("Unexpected line $line");
                }
            };
            $handle->push_read(line => $code);
        }
    };
}

{
    my $generator = sub {
        my ($self, $cmd) = @_;
        my $memcached = $self->memcached;
        sub {
            my ($key, $value, $exptime, $noreply, $cb) = @_;
            my $handle = $self->get_handle_for( $key );

            my $flags = 0;
            if (ref $value) {
                $value = Storable::nfreeze($value);
                $flags |= AnyEvent::Memcached::Protocol::F_STORABLE();
            }

            my $len = bytes::length($value);
            my $threshold = $memcached->compress_threshold;
            my $compressable = 
                ($cmd ne 'append' && $cmd ne 'prepend') &&
                $threshold && 
                AnyEvent::Memcached::Protocol::HAVE_ZLIB() &&
                $memcached->compress_enabled &&
                $len >= $threshold
            ;
            if ($compressable) {
                my $c_val = Compress::Zlib::memGzip($value);
                my $c_len = length($c_val);

                if ($c_len < $len * ( 1 - AnyEvent::Memcached::Protocol::COMPRESS_SAVINGS() ) ) {
                    $value = $c_val;
                    $len = $c_len;
                    $flags |= AnyEvent::Memcached::Protocol::F_COMPRESS();
                }
            }
            $exptime = int($exptime || 0);
            $handle->push_write("$cmd $key $flags $exptime $len\r\n$value\r\n");
            $handle->push_read(regex => qr{^STORED\r\n}, sub {
                $cb->(1) if $cb;
                $memcached->drain_queue;
            });
        };
    };

    sub _build_add_cb {
        my $self = shift;
        return $generator->( $self, "add" );
    }

    sub _build_replace_cb {
        my $self = shift;
        return $generator->( $self, "replace" );
    }

    sub _build_set_cb {
        my $self = shift;
        return $generator->( $self, "set" );
    }
}

sub _build_stats_cb {
    my $self = shift;
    return sub {
        my ($name, $cb) = @_;

        my %rv;
        my $cv = AE::cv {
            $cb->( \%rv );
        };
        my $memcached = $self->memcached;

        foreach my $server ($memcached->all_servers) {
            my $handle = $memcached->get_handle( $server );

            $cv->begin;
            $handle->push_write( $name ? "stats $name\r\n" : "stats\r\n" );
            my $code; $code = sub {
                my ($handle, $line) = @_;
                if ($line eq 'END') {
                    $cv->end;
                } elsif ( $line =~ /^STAT (\S+) (\S+)$/) {
                    $rv{ $server }->{ $1 } = $2;
                    $handle->push_read( line => $code );
                }
            };
            $handle->push_read( line => $code );
        }
    }
}

__PACKAGE__->meta->make_immutable();

1;

__END__

=head1 NAME

AnyEvent::Memcached::Protocol::Text - Implements Memcached Text Protocol

=cut
